# Make sure specs run with the definitions from test.rb
environment = ENV['ROBOT_ENVIRONMENT'] = 'test'

bootfile = File.expand_path(File.dirname(__FILE__) + '/../config/boot')
require bootfile

require 'rspec'

LyberCore::Log.set_logfile("#{ROBOT_ROOT}/log/workflow_archiver.log")
LyberCore::Log.set_level(1)