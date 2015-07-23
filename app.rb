require 'sinatra'
require 'json'
require 'koala'
require 'redis'
require 'rest-client'
require 'active_support/all'
require 'sanitize'

configure do
  set :redis, Redis.new(host: '127.0.0.1', port: '6379', db: 1)
  (1..3).each do |i|
    Thread.new do
      redis = Redis.new(host: '127.0.0.1', port: '6379', db: 1)
      log "Thread ##{i} started" 
      loop do
        query = redis.brpop('queries').last
        begin
          log "Thread ##{i} processing query: #{query}"
          parsed_query = JSON.parse query
          cognitive_search parsed_query['query'], redis, parsed_query['token']
          log "Thread ##{i} finished query: #{query}"
        rescue => e
          log "Thread ##{i} can't process query #{query}: #{$!}"
          puts "Backtrace:\n\t#{e.backtrace.join("\n\t")}"
        end
      end
    end
  end
end

get '/' do
  erb :index
end

post '/search' do
  if params[:q].present?
    redis.lpush 'queries', { query: params[:q], token: request.cookies['facebook_access_token'] }.to_json
    { data: 'success' }.to_json    
  else
    { error: 'blank_query' }.to_json
  end
end

get '/search_results' do
  if params[:q].present?
    md5 = "results_#{Digest::MD5.hexdigest params[:q]}"
    results = JSON.parse(redis.get md5) rescue {'status' => 'in_progress', 'results' => {}, 'progress' => 0}
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
    results = JSON.parse(redis.get md5) rescue {'status' => 'in_progress', 'results' => {}, 'progress' => 0}
    unless results['status'] == 'finished'
      results = {'status' => 'in_progress', 'results' => {}, 'progress' => 0}
      yandex = Hash.from_xml RestClient.get("https://yandex.com/search/xml?user=grophen&key=03.43282533:5e955fb84f7bf3dddd1ab1b14cc6eaa9&query=#{ERB::Util.url_encode query}&l10n=en&sortby=rlv&filter=moderate&groupby=attr%3D%22%22.mode%3Dflat.groups-on-page%3D100.docs-in-group%3D1")
      yandex['yandexsearch']['response']['results']['grouping']['group'].each do |doc|
        results['progress'] = results['progress'].to_i + 1
        url = doc['doc']['url']
        log "Processing URL: #{url}"
        relext = get_relext url, redis
        if relext
          names = relext['doc']['mentions']['mention'].select {|m| m['role'] == 'PERSON' and m['mtype'] = 'NAM' and m['text'] =~ /\A([A-Z][a-z]+\s?){2,4}\Z/ }.map {|m| m['text'] } rescue []
          names.each do |name|
            checked_name = check_name!(name, redis, token)
            if checked_name and checked_name != 'false'
              results['results'][checked_name] ||= []
              results['results'][checked_name] << url
              results['results'][checked_name] = results['results'][checked_name].uniq
            end
          end
          redis.setex(md5, 24*3600, results.to_json)
        end
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
    @graph ||= Koala::Facebook::API.new token
    parts = name.split(/\s/)
    checked_name, fb_names = if parts.count == 2
      [name, @graph.get_object('search', q: name, type: 'user')]
    elsif parts.count == 3
      fb_names1 = @graph.get_object('search', q: "#{parts[0]} #{parts[1]}", type: 'user')
      fb_names2 = @graph.get_object('search', q: "#{parts[1]} #{parts[2]}", type: 'user')
      if fb_names1.any?
        ["#{parts[0]} #{parts[1]}", fb_names1]
      elsif fb_names2.any?
        ["#{parts[1]} #{parts[2]}", fb_names2]
      end
    elsif parts.count == 4
      fb_names1 = @graph.get_object('search', q: "#{parts[0]} #{parts[1]}", type: 'user')
      fb_names2 = @graph.get_object('search', q: "#{parts[1]} #{parts[2]}", type: 'user')
      fb_names3 = @graph.get_object('search', q: "#{parts[2]} #{parts[3]}", type: 'user')
      if fb_names1.any?
        ["#{parts[0]} #{parts[1]}", fb_names1]
      elsif fb_names2.any?
        ["#{parts[1]} #{parts[2]}", fb_names2]
      elsif fb_names3.any?
        ["#{parts[2]} #{parts[3]}", fb_names3]
      end
    end
    if fb_names.present? and fb_names.map {|fb_name| levenshtein_distance checked_name, fb_name['name'] }.min < 2
      redis.setex(md5, 7*24*3600, checked_name)
      checked_name
    else
      redis.setex(md5, 7*24*3600, false)
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

def get_relext url, redis
  md5 = "relext_#{Digest::MD5.hexdigest url}"
  relext = redis.get md5
  unless relext 
    content_type = RestClient.head(url).headers[:content_type] rescue nil
    if content_type.to_s =~ /text\/html/
      text = RestClient.get(url) rescue nil
      text = CGI.unescapeHTML(Sanitize.fragment(text.to_s, remove_contents: [:link, :style, :script]).squish) rescue nil
      relext = RestClient.post("http://ambroi.eu-gb.mybluemix.net/say/relext", text: text[0..15000]) rescue nil
      redis.setex(md5, 7*24*3600, relext) if relext.present?
    end
  end
  JSON.parse(relext) rescue nil
end

def log msg
  puts "#{Time.now} === #{msg}"
  STDOUT.flush
end

def redis
  settings.redis
end
