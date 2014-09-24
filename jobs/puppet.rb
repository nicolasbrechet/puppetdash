require 'json'
require 'net/http'
require 'uri'

last_manhosts = 0
last_manresources = 0
last_avgresources = 0

# :first_in sets how long it takes before the job is first run. In this case, it is run immediately
SCHEDULER.every '30s', :first_in => 0, allow_overlapping: false do |puppet|

  time_past = (Time.now - 86400)
  #time_past = (Time.now - 1800)
  ftime_now = Time.now.strftime("%FT%T")
  ftime_past = time_past.strftime("%FT%T")
  
  @failedhosts = []
  @failed = 0
  @changed = 0
  @unchanged = 0
  @pending = 0
  @eventtext = ''
  
  nodes = JSON.parse(Net::HTTP.get_response(URI.parse('http://localhost:18080/v3/nodes/')).body)


  numberofhosts = JSON.parse(
                    Net::HTTP.get_response(
                      URI.parse('http://localhost:18080/v3/metrics/mbean/com.puppetlabs.puppetdb.query.population:type=default,name=num-nodes')).body)["Value"]


  numberofresources = JSON.parse(
                        Net::HTTP.get_response(
                          URI.parse('http://localhost:18080/v3/metrics/mbean/com.puppetlabs.puppetdb.query.population:type=default,name=num-resources')).body)["Value"]


  avgresources  = JSON.parse(
                    Net::HTTP.get_response(
                      URI.parse('http://localhost:18080/v3/metrics/mbean/com.puppetlabs.puppetdb.query.population:type=default,name=avg-resources-per-node')).body)["Value"].round

  
  last_manhosts = numberofhosts
  last_manresources = numberofresources
  last_avgresources = avgresources
                      
  nodes.each do |node|
    uri = URI.parse('http://localhost:18080/v3/event-counts/')
    uri.query = URI.encode_www_form(:query => %Q'["and",["=", "certname", "#{node['name']}"],["=", "latest-report?", "true"]]', :'summarize-by' => 'certname', :'count-by' => 'resource')
    #uri.query = URI.encode_www_form(:query => %Q'["and",["=", "certname", "#{node['name']}"],["<", "timestamp", "#{ftime_now}"],[">", "timestamp", "#{ftime_past}"],["=", "latest-report?", "true"]]', :'summarize-by' => 'certname', :'count-by' => 'resource')
                                   

    events = JSON.parse(Net::HTTP.get_response(uri).body)
    events.each do |event|
      if event['failures'] > 0
        @failedhosts << event['subject']['title']

        @failed += 1
      elsif event['noops'] > 0
        @pending += 1
      elsif event['successes'] > 0
        @changed += 1
      end
  
    end
  end
  
  send_event('pupfailed',     {value: @failed,  max: numberofhosts})
  send_event('puppending',    {value: @pending, max: numberofhosts})
  send_event('pupchanged',    {value: @changed, max: numberofhosts})
  send_event('manhosts',      {current: numberofhosts, last:last_manhosts})
  send_event('manresources',  {current: numberofresources, last:last_manresources})
  send_event('avgresources',  {current: avgresources, last:last_avgresources})
  
  @failedhosts.each do |host|
    @eventtext<< "#{host} \n"
  end

  send_event('failedhosts', { text: @eventtext })

end