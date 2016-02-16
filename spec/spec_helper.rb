# Make sure specs run with the definitions from test.rb
ENV['ROBOT_ENVIRONMENT'] = 'test'

require 'rspec'
require 'workflow-archiver'

LyberCore::Log.set_logfile(STDERR)
LyberCore::Log.set_level(1)

$sequel_db = Sequel.sqlite

Dor::WorkflowArchiver.config.configure do
  dor_service_uri 'http://example.com'
end

$sequel_db.create_table? :workflow do
  primary_key :id
  String :druid
  String :datastream
  String :process
  String :status
  String :error_msg
  String :error_txt
  DateTime :datetime
  Integer :attempts
  Decimal :elapsed
  String :lifecycle
  String :repository
  String :note
  Integer :priority
  String :lane_id
end

$sequel_db.create_table? :workflow_archive do
  primary_key :id
  String :druid
  String :datastream
  String :process
  String :status
  String :error_msg
  String :error_txt
  DateTime :datetime
  Integer :attempts
  Decimal :elapsed
  String :lifecycle
  String :repository
  DateTime :archive_dt
  Integer :version
  String :note
  Integer :priority
  String :lane_id
end
