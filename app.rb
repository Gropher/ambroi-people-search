require 'sinatra'
require 'json'
require 'koala'
require 'redis'
require 'rest-client'
require 'active_support/all'


REDIS_CONFIG={ host: '127.0.0.1', port: '6379', db: 1 }
TOKEN="AQX8dPqwsBfIifJgQ1xxporlWCaI5DlClITsScweW9VXf2SKQ5oQRkpf0nyUheKApfDKcfb3SpqlOBcs6ak0W8K6oafey1GI-yl9cY7J4hRhcpH82RS1r0BYrdjwugbylXrGa3XFXgT4drdAlwdE6Zv-68Fqb7swTP1xEyInnzGw5VFvuYo"

configure do
  set :redis, Redis.new(REDIS_CONFIG)
  (1..5).each do |i|
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
          log "Search Thread ##{i} backtrace:\n\t#{e.backtrace.join("\n\t")}"
        end
      end
    end
  end
  (1..1).each do |i|
    Thread.new do
      redis = Redis.new(REDIS_CONFIG)
      log "LinkedIn profile Thread ##{i} started" 
      loop do
        query = redis.brpop('linkedin_profile_requests').last
        begin
          log "LinkedIn profile Thread ##{i} processing query: #{query}"
          parsed_query = JSON.parse query
          get_linkedin_profile parsed_query['url'], redis, parsed_query['token']
          log "LinkedIn profile Thread ##{i} finished query: #{query}"
        rescue => e
          log "LinkedIn profile Thread ##{i} can't process query #{query}: #{$!}"
          log "LinkedIn profile Thread ##{i} backtrace:\n\t#{e.backtrace.join("\n\t")}"
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
      yandex = Hash.from_xml RestClient.get("https://yandex.com/search/xml?user=grophen&key=03.43282533:5e955fb84f7bf3dddd1ab1b14cc6eaa9&query=#{ERB::Util.url_encode query}&l10n=en&sortby=rlv&filter=moderate&groupby=attr%3D%22%22.mode%3Dflat.groups-on-page%3D100.docs-in-group%3D1&site=linkedin.com/in")
      urls = yandex['yandexsearch']['response']['results']['grouping']['group'].map {|doc| doc['doc']['url'] }
      urls.each do |url| 
        redis.lpush 'linkedin_profile_requests', { url: url, token: token }.to_json
      end
      urls.each do |url|
        linkedin_profile = nil
        loop do 
          linkedin_profile = redis.get "linkedin_profile_#{Digest::MD5.hexdigest url}"
          if linkedin_profile
            log "'#{query}' search got linkedin profile for #{url}"
            break
          else
            log "'#{query}' search is waiting for linkedin profile for #{url}"
            sleep 5
          end
        end
        linkedin_profile = JSON.parse linkedin_profile
        results['results'] << linkedin_profile if linkedin_profile.present?
        results['progress'] = results['progress'].to_i + 1
        log "'#{query}' search progress is #{results['progress']}%"
        redis.setex(md5, 24*3600, results.to_json)
      end
      results['status'] = 'finished'
      redis.setex(md5, 3600, results.to_json)
    end
  end
end

def check_name! name, redis, token
  return false if name == 'private private'
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
    if fb_names.any? and fb_names.map {|fb_name| levenshtein_distance checked_name, fb_name['name'] }.min < 2
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

def get_linkedin_profile profile_url, redis, token
  md5 = "linkedin_profile_#{Digest::MD5.hexdigest profile_url}"
  unless redis.exists md5
    sleep 5
    url = profile_url.split('?').first.gsub('/people/url=', '')
    url = "https://api.linkedin.com/v1/people/#{ {url: url}.to_query }:(id,first-name,last-name,location:(name),headline,summary,positions,specialties,educations,skills,industry,picture-url,public-profile-url)?#{ {oauth2_access_token: TOKEN}.to_query }"
    linkedin_profile = begin
      Hash.from_xml(RestClient.get(url))['person']
    rescue => e
      log "get_linkedin_profile error: #{$!}"
      log "get_linkedin_profile backtrace:\n\t#{e.backtrace.join("\n\t")}"
      if e.http_code == 403
        { 'id' => 'forbidden', 'first_name' => 'forbidden', 'last_name' => 'forbidden' }
      else
        nil
      end
    end
    if linkedin_profile
      name = "#{linkedin_profile['first_name']} #{linkedin_profile['last_name']}"
      log "LinkedIn profile checking name #{name}"
      checked_name = begin
        if name == 'private private' or name == 'forbidden forbidden'
          if profile_url =~ /linkedin\.com\/in\//
            full_name = profile_url.split('/').last
	    linkedin_profile['headline'] = linkedin_profile['id']
	    linkedin_profile['location'] = { 'name' => linkedin_profile['id'] }
	    linkedin_profile['first_name'] = full_name.split('-')[0].to_s
	    linkedin_profile['last_name'] = full_name.split('-')[1].to_s
	    linkedin_profile['public_profile_url'] = profile_url
            true
	  else
	    false
          end
        else
	  check_name!(name, redis, token) 
        end
      rescue
        log "LinkedIn profile Facebook Error while checking name #{name}" 
        false
      end
      if checked_name != false and checked_name != 'false'
        if linkedin_profile['id'] == 'forbidden'
          redis.setex(md5, 24*3600, linkedin_profile.to_json)
        else
          redis.setex(md5, 30*24*3600, linkedin_profile.to_json)
        end
      else
        redis.setex(md5, 24*3600, [].to_json)
      end
    else
      redis.setex(md5, 24*3600, [].to_json)
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
  { 'status' => 'in_progress', 'results' => [], 'progress' => 0 }.dup
end

def redis
  settings.redis
end
