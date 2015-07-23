require 'sinatra'
require 'json'
require 'koala'
require 'redis'
require 'rest-client'
require 'active_support/all'
require 'sanitize'


REDIS_CONFIG={ host: '127.0.0.1', port: '6379', db: 1 }

configure do
  set :redis, Redis.new(REDIS_CONFIG)
  (1..3).each do |i|
    Thread.new do
      redis = Redis.new(REDIS_CONFIG)
      log "Search Thread ##{i} started" 
      loop do
        query = redis.brpop('queries').last
        begin
          log "Search Thread ##{i} processing query: #{query}"
          parsed_query = JSON.parse query
          cognitive_search parsed_query['query'], redis, parsed_query['token']
          log "Search Thread ##{i} finished query: #{query}"
        rescue => e
          log "Search Thread ##{i} can't process query #{query}: #{$!}"
          puts "Search Thread ##{i} backtrace:\n\t#{e.backtrace.join("\n\t")}"
        end
      end
    end
  end
  (1..4).each do |i|
    Thread.new do
      redis = Redis.new(REDIS_CONFIG)
      log "RelExt Thread ##{i} started" 
      loop do
        query = redis.brpop('relext_requests').last
        begin
          log "RelExt Thread ##{i} processing query: #{query}"
          parsed_query = JSON.parse query
          get_relext parsed_query['url'], redis, parsed_query['token']
          log "RelExt Thread ##{i} finished query: #{query}"
        rescue => e
          log "RelExt Thread ##{i} can't process query #{query}: #{$!}"
          puts "RelExt Thread ##{i} backtrace:\n\t#{e.backtrace.join("\n\t")}"
        end
      end
    end
  end
end

get '/' do
  erb :index
end

post '/search' do
  query = normalize_query params[:q]
  token = request.cookies['facebook_access_token']
  if query.present?
    graph = Koala::Facebook::API.new token
    graph.get_object('/me')
    redis.lpush 'queries', { query: query, token: token }.to_json
    { data: 'success' }.to_json    
  else
    { error: 'blank_query' }.to_json
  end
end

get '/search_results' do
  query = normalize_query params[:q]
  if query.present?
    md5 = "results_#{Digest::MD5.hexdigest query}"
    results = JSON.parse(redis.get md5) rescue empty_results
    results['results'] = results['results'].map do |name, links|
      { name: name, links: links }
    end.compact.to_a.sort {|a,b| b[:links].count <=> a[:links].count } 
    { data: results }.to_json
  else
    { error: 'blank_query' }.to_json
  end
end

def cognitive_search query, redis, token
  if query
    md5 = "results_#{Digest::MD5.hexdigest query}"
    results = JSON.parse(redis.get md5) rescue empty_results
    unless results['status'] == 'finished'
      results = empty_results
      yandex = Hash.from_xml RestClient.get("https://yandex.com/search/xml?user=grophen&key=03.43282533:5e955fb84f7bf3dddd1ab1b14cc6eaa9&query=#{ERB::Util.url_encode query}&l10n=en&sortby=rlv&filter=moderate&groupby=attr%3D%22%22.mode%3Dflat.groups-on-page%3D100.docs-in-group%3D1")
      urls = yandex['yandexsearch']['response']['results']['grouping']['group'].map {|doc| doc['doc']['url'] }
      urls.each do |url| 
        redis.lpush 'relext_requests', { url: url, token: token }.to_json
      end
      urls.each do |url|
        relext = nil
        loop do 
          relext = redis.get "relext_#{Digest::MD5.hexdigest url}"
          if relext
	    log "'#{query}' search got RelExt for #{url}"
            break
          else
	    log "'#{query}' search is waiting for RelExt for #{url}"
            sleep 5
          end
        end
        relext = JSON.parse relext
        relext.each do |name|
          results['results'][name] ||= []
          results['results'][name] << url
          results['results'][name] = results['results'][name].uniq
        end
        results['progress'] = results['progress'].to_i + 1
        log "'#{query}' search progress is #{results['progress']}%"
        redis.setex(md5, 24*3600, results.to_json)
      end
      results['status'] = 'finished'
      redis.setex(md5, 24*3600, results.to_json)
    end
  end
end

def check_name! name, redis, token
  md5 = "is_a_name_#{Digest::MD5.hexdigest name}"
  if redis.exists(md5)
    redis.get(md5)
  else
    graph = Koala::Facebook::API.new token
    parts = name.split(/\s/)
    checked_name, fb_names = if parts.count == 2
                               [name, graph.get_object('search', q: name, type: 'user')]
                             elsif parts.count == 3
                               fb_names1 = graph.get_object('search', q: "#{parts[0]} #{parts[1]}", type: 'user')
                               fb_names2 = graph.get_object('search', q: "#{parts[1]} #{parts[2]}", type: 'user')
                               if fb_names1.any?
                                 ["#{parts[0]} #{parts[1]}", fb_names1]
                               elsif fb_names2.any?
                                 ["#{parts[1]} #{parts[2]}", fb_names2]
                               end
                             elsif parts.count == 4
                               fb_names1 = graph.get_object('search', q: "#{parts[0]} #{parts[1]}", type: 'user')
                               fb_names2 = graph.get_object('search', q: "#{parts[1]} #{parts[2]}", type: 'user')
                               fb_names3 = graph.get_object('search', q: "#{parts[2]} #{parts[3]}", type: 'user')
                               if fb_names1.any?
                                 ["#{parts[0]} #{parts[1]}", fb_names1]
                               elsif fb_names2.any?
                                 ["#{parts[1]} #{parts[2]}", fb_names2]
                               elsif fb_names3.any?
                                 ["#{parts[2]} #{parts[3]}", fb_names3]
                               end
                             end
    if fb_names.present? and fb_names.map {|fb_name| levenshtein_distance checked_name, fb_name['name'] }.min < 2
      redis.setex(md5, 30*24*3600, checked_name)
      checked_name
    else
      redis.setex(md5, 30*24*3600, false)
      false
    end
  end
end

def levenshtein_distance(a, b)
  a, b = a.downcase, b.downcase
  costs = Array(0..b.length) # i == 0
  (1..a.length).each do |i|
    costs[0], nw = i, i - 1  # j == 0; nw is lev(i-1, j)
    (1..b.length).each do |j|
      costs[j], nw = [costs[j] + 1, costs[j-1] + 1, a[i-1] == b[j-1] ? nw : nw + 1].min, costs[j]
    end
  end
  costs[b.length]
end

def get_relext url, redis, token
  md5 = "relext_#{Digest::MD5.hexdigest url}"
  unless redis.exists md5
    content_type = RestClient.head(url).headers[:content_type] rescue nil
    if content_type.to_s =~ /text\/html/
      text = RestClient.get(url) rescue nil
      text = CGI.unescapeHTML(Sanitize.fragment(text.to_s, remove_contents: [:link, :style, :script]).squish) rescue nil
      relext = if text
        JSON.parse(RestClient.post("http://ambroi.eu-gb.mybluemix.net/say/relext", text: text[0..10000])) rescue nil
      end
      relext = relext['doc']['mentions']['mention'].select {|m| m['role'] == 'PERSON' and m['mtype'] == 'NAM' and m['text'] =~ /\A([A-Z][a-z]+\s?){2,4}\Z/ }.map {|m| m['text'] } rescue []
        relext = relext.map do |name|
          log "RelExt checking name #{name}"
          checked_name = begin
            check_name!(name, redis, token) 
          rescue
            log "RelExt Facebook Error while checking name #{name}" 
            name
          end
          checked_name if checked_name != false and checked_name != 'false'
        end.compact
      redis.setex(md5, 30*24*3600, relext.to_json)
    else
      redis.setex(md5, 30*24*3600, [].to_json)
    end
  end
end

def normalize_query query
  query.to_s.strip.downcase.squish
end

def log msg
  puts "#{Time.now} #{'='*20} #{msg}"
  STDOUT.flush
end

def empty_results
  { 'status' => 'in_progress', 'results' => {}, 'progress' => 0 }.dup
end

def redis
  settings.redis
end
