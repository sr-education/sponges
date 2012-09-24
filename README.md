# sponges
When I build some worker, I want them to be like an army of spongebob, always
stressed and eager to work. sponges helps you to build this army of sponge, to
control them, and, well, kill them gracefully.

## Installation

Ruby 1.9.3 is required.

Install it with rubygems:

    gem install sponges

With bundler, add it to your `Gemfile`:

``` ruby
gem "sponges"
```

## Usage
``` ruby
# The worker class is the one you want to daemonize.
#
require 'sponges'

class Worker
  def run
    puts Process.pid
    sleep 1
    run
  end
end

options = {
  size: 3,
  log_dir: "/tmp"
}

Sponges::Runner.new("bob").work(worker.new, :run)
```

## TODO

* Logging solution
* Specing
* Storing pids
* Bin

## Copyright

MIT. See LICENSE for further details.

  ___ _ __   ___  _ __   __ _  ___  ___
 / __| '_ \ / _ \| '_ \ / _` |/ _ \/ __|
 \__ \ |_) | (_) | | | | (_| |  __/\__ \
 |___/ .__/ \___/|_| |_|\__, |\___||___/
     | |                 __/ |
     |_|                |___/

