require 'spec_helper'

describe Dor::WorkflowArchiver do
  before(:each) do
    # set login, passwords, uri
    @login = ''
    @pword = ''
    @uri = ''

    @conn = double('db_conn')
  end

  describe '#get_current_version' do
    let!(:archiver) do
      Dor::WorkflowArchiver.new(login: @login, password: @pword, db_uri: @uri, retry_delay: 1,
                                dor_service_uri: 'http://sul-lyberservices-dev.stanford.edu')
    end

    it "calls the DOR REST service to get the object's latest version" do
    end
  end
end
