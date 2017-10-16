# Running asynchronous workers to process graphite metrics
### This is meant as a proof of concept only. Not to be used in production.

There are 3 components that can be spread out across multiple host

### Step 1 - Redis host(s)

`sudo apt-get install redis-server`

Edit etc/redis/redis.conf.  Update bind configuration to:

bind 0.0.0.0

`sudo service redis-server restart`

### Step 2 - Graphite sink(s)

```
git clone https://github.com/burnsie7/graphite-example.git
cd graphite-example
sudo apt-get update
sudo apt-get install supervisor
sudo apt-get install python-pip
sudo pip install celery
sudo pip install redis
sudo pip install datadog
sudo pip install tornado
```
Edit tasks.py
Update the following link with the IP or hostname of the Redis server configured in step 1 OR set a env var export REDIS_HOST=<YOUR_REDIS_HOST>.

REDIS_HOST = os.getenv('REDIS_HOST', '<YOUR_REDIS_HOST>')

Edit /etc/supervisor/conf.d/supervisor.conf.  Add the following, updating 'numprocs'.
```
[program:graphite-sink]
command=python /exact/path/to/graphite-example/graphite.py 1731%(process_num)01d
process_name=%(program_name)s_%(process_num)01d
redirect_stdout=true
user=ubuntu
stdout_logfile=/var/log/gsink-%(process_num)01d.log
numprocs=<UPDATE W NUM PROCS YOU WANT TO USE>
```

Update supervisor and restart all services.

```
sudo supervisorctl
update
restart all
```

### Step 3 - Worker hosts

Install requirements.

```
sudo DD_API_KEY=<YOUR_API_KEY> bash -c "$(curl -L https://raw.githubusercontent.com/DataDog/dd-agent/master/packaging/datadog-agent/source/install_agent.sh)"
sudo /opt/datadog-agent/embedded/bin/pip install celery
sudo /opt/datadog-agent/embedded/bin/pip install redis
git clone https://github.com/burnsie7/graphite-example.git
sudo cp graphite-example/tasks.py /opt/datadog-agent/agent/
```

Update agent config.

Edit /etc/dd-agent/supervisor.conf and add the following above the [group:datadog-agent] section at end of file, updating 'numprocs'.

```
[program:celery-worker]
command=/opt/datadog-agent/embedded/bin/celery -A tasks worker -l info
user=dd-agent
autorestart=true
autostart=true
stdout_logfile_maxbytes=0
stdout_logfile_backups=0
stdout_logfile=/var/log/datadog/%(program_name)s.log
redirect_stderr=true
stopasgroup=true
killasgroup=true
numprocs=<UPDATE W NUM PROCS YOU WANT TO USE>
process_name=%(program_name)s_%(process_num)s
```

Add 'celery-worker' to the [group:datadog-agent] section.  Don't copy/paste as your programs may not be identical.

```
[group:datadog-agent]
programs=forwarder,collector,dogstatsd,jmxfetch,go-metro,trace-agent,process-agent,celery-worker
```

Edit /opt/datadog-agent/agent/tasks.py. Update the following link with the IP or hostname of the Redis server configured in step 1 OR set a env var export REDIS_HOST=<YOUR_REDIS_HOST>.

REDIS_HOST = os.getenv('REDIS_HOST', '<YOUR_REDIS_HOST>')

Restart the datadog agent:

`sudo /etc/init.d/datadog-agent restart`

You can view the status of the individual processes using:

`sudo /opt/datadog-agent/embedded/bin/python2.7 /opt/datadog-agent/bin/supervisorctl -c /etc/dd-agent/supervisor.conf`

### Step 4 - Your carbon-relay

Point your carbon relay at the graphite sinks specified in step 2.  Note that the number of sinks on an individual host is configured by 'numprocs' in /etc/supervisor/conf.d/supervisor.conf.  The port of the first sink will be 17310 and the port will increment for additional procs.  For example, if numprocs were set to 4:

sink-hostname:17310
sink-hostname:17311
sink-hostname:17312
sink-hostname:17313

There are different options for distributing carbon relay, whether set with destinations directly in the carbon config or using haproxy.  Distribute the requests across the different sinks you have configured.

If using relay rules it is advantageous to send only the metrics you wish to see in datadog to the sinks.  For example:

[datadog]
pattern = ^zxyzxy\.webapp.+
destinations = haproxy:port

[default]
default = true
destinations = 127.0.0.1:2004
