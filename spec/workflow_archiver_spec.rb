require 'spec_helper'
require 'yaml'

describe Dor::WorkflowArchiver do
  subject do
    Dor::WorkflowArchiver.new(db_connection: $sequel_db)
  end

  before do
    workflows = subject.conn.from(:workflow)

    data = YAML.load <<-EOF
---
- :druid: integration:345
  :datastream: googleScannedBookWF
  :process: cleanup
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: googleScannedBookWF
  :process: register
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: googleScannedBookWF
  :process: sdr-ingest-archive
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: googleScannedBookWF
  :process: register-object
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: etdSubmitWF
  :process: cleanup
  :status: waiting
  :repository: dor
- :druid: integration:345
  :datastream: etdSubmitWF
  :process: register
  :status: completed
  :repository: dor
- :druid: integration:345
  :datastream: sdrIngestWF
  :process: register-sdr
  :status: completed
  :repository: sdr
- :druid: integration:678
  :datastream: googleScannedBookWF
  :process: cleanup
  :status: waiting
  :repository: dor
- :druid: integration:678
  :datastream: googleScannedBookWF
  :process: register
  :status: completed
  :repository: dor
- :druid: integration:568
  :datastream: sdrIngestWF
  :process: cleanup
  :status: completed
  :repository: sdr
- :druid: integration:999
  :datastream: etdSubmitWF
  :process: cleanup
  :status: completed
  :repository: sdr
- :druid: integration:678
  :datastream: sdrIngestWF
  :process: register-sdr
  :status: completed
  :repository: 
    EOF

    data.each do |d|
      workflows.insert d
    end

  end

  after do
    subject.conn.from(:workflow).where(Sequel.like(:druid, 'integration:%')).delete
    subject.conn.from(:workflow_archive).where(Sequel.like(:druid, 'integration:%')).delete
  end

  describe "#find_completed_objects" do
    let(:rows) { subject.find_completed_objects.to_a }

    it 'retrieves objects that have completed all workflow steps' do
      expect(rows.length).to eq 5
    end

    it 'includes objects that have completed all workflow steps for a given workflow' do
      expect(rows).to include(repository: 'dor', datastream: 'googleScannedBookWF', druid: 'integration:345')
    end

    it 'excludes objects that have not completed all workflow steps' do
      expect(rows).not_to include(repository: 'dor', datastream: 'googleScannedBookWF', druid: 'integration:678')
      expect(rows).not_to include(repository: 'dor', datastream: 'etdSubmitWF', druid: 'integration:345')
    end
  end

  describe '#archive_rows' do
    before do
      expect(Faraday).to receive(:get).at_least(:twice).with(/^#{Dor::WorkflowArchiver.config.dor_service_uri}\/dor\/v1\/objects\/integration:/).and_return('1')
    end

    it 'copies completed workflow rows to the archive table' do
      expect { subject.archive }.to change { subject.conn.from(:workflow_archive).count }.from(0).to(8)
      expect(subject.find_completed_objects.count).to eq 0

      archived = subject.conn.from(:workflow_archive).to_a

      expect(archived).to include(hash_including(druid: 'integration:678', datastream: 'sdrIngestWF'))
      expect(archived).not_to include(hash_including(druid: 'integration:678', datastream: 'googleScannedBookWF'))
    end
  end
end
