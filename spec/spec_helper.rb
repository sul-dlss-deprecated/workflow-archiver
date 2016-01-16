require 'rubygems'
require 'bundler/setup'

# Make sure specs run with the definitions from test.rb
ENV['ROBOT_ENVIRONMENT'] = 'test'

require 'rspec'
require 'dor/workflow_archiver'

LyberCore::Log.set_logfile(STDERR)
LyberCore::Log.set_level(1)
