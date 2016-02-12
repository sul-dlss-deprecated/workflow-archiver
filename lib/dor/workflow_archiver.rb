require 'rest_client'
require 'confstruct'
require 'lyber_core'
require 'sequel'

module Dor
  # Holds the paramaters about the workflow rows that need to be deleted
  ArchiveCriteria = Struct.new(:repository, :druid, :datastream, :version) do
    # @param [Array<Hash>] List of objects returned from {WorkflowArchiver#find_completed_objects}.  It expects the following keys in the hash
    def setup_from_query(row_hash)
      self.repository = row_hash[:repository]
      self.druid = row_hash[:druid]
      self.datastream = row_hash[:datastream]
      set_current_version
      self
    end

    # Removes version from list of members, then picks out non nil members and builds a hash of column_name => column_value
    # @return [Hash] Maps column names (in ALL caps) to non-nil column values
    def to_bind_hash
      h = {}
      members.reject { |mem| mem =~ /version/ }.each do |m|
        h[m] = send(m) if send(m)
      end
      h
    end

    def set_current_version
      self.version = RestClient.get WorkflowArchiver.config.dor_service_uri + "/dor/v1/objects/#{druid}/versions/current"
    rescue RestClient::InternalServerError => ise
      raise unless ise.inspect =~ /Unable to find.*in fedora/
      LyberCore::Log.warn ise.inspect.to_s
      LyberCore::Log.warn "Moving workflow rows with version set to '1'"
      self.version = '1'
    end
  end

  class WorkflowArchiver
    WF_COLUMNS = %w(id druid datastream process status error_msg error_txt datetime attempts lifecycle elapsed repository note priority lane_id)

    # These attributes mostly used for testing
    attr_reader :errors

    def self.config
      @@conf ||= Confstruct::Configuration.new
    end

    # Sets up logging and connects to the database.  By default it reads values from constants:
    #  WORKFLOW_DB_LOGIN, WORKFLOW_DB_PASSWORD, WORKFLOW_DB_URI, DOR_SERVICE_URI but can be overriden with the opts Hash
    # @param [Hash] opts Options to override database parameters
    # @option opts [String] :db_uri ('WORKFLOW_DB_URI') Database uri
    # @option opts [String] :wf_table ('workflow') Name of the active workflow table
    # @option opts [String] :wfa_table ('workflow_archive') Name of the workflow archive table
    # @option opts [String] :dor_service_uri ('DOR_SERVICE_URI') URI of the DOR Rest service
    # @option opts [Integer] :retry_delay (5) Number of seconds to sleep between retries of database operations
    def initialize(opts = {})
      @conn = opts[:db_connection]
      @db_uri                 = opts.fetch(:db_uri, WorkflowArchiver.config.db_uri).freeze
      @dor_service_uri        = opts.include?(:dor_service_uri) ? opts[:dor_service_uri] : WorkflowArchiver.config.dor_service_uri
      @workflow_table         = opts.include?(:wf_table)    ? opts[:wf_table]    : 'workflow'
      @workflow_archive_table = opts.include?(:wfa_table)   ? opts[:wfa_table]   : 'workflow_archive'
      @retry_delay            = opts.include?(:retry_delay) ? opts[:retry_delay] : 5
      # initialize some counters
      @errors = 0
      @archived = 0
    end

    def conn
      @conn ||= Sequel.connect(@db_uri)
    end

    # @return [String] The columns appended with comma and newline
    def wf_column_string
      WF_COLUMNS.join(",\n")
    end

    # @return [String] The columns prepended with 'w.' and appended with comma and newline
    def wf_archive_column_string
      WF_COLUMNS.map { |col| "#{@workflow_table}.#{col}" }.join(",\n")
    end

    # Use this as a one-shot method to archive all the steps of an object's particular datastream
    #   It will connect to the database, archive the rows, then logoff.  Assumes caller will set version (like the Dor REST service)
    # @note Caller of this method must handle destroying of the connection pool
    # @param [String] repository
    # @param [String] druid
    # @param [String] datastream
    # @param [String] version
    def archive_one_datastream(repository, druid, datastream, version)
      criteria = [ArchiveCriteria.new(repository, druid, datastream, version)]
      archive_rows criteria
    end

    # Copies rows from the workflow table to the workflow_archive table, then deletes the rows from workflow
    # Both operations must complete, or they get rolled back
    # @param [Array<ArchiveCriteria>] objs List of objects returned from {#find_completed_objects} and mapped to an array of ArchiveCriteria objects.
    def archive_rows(objs)
      objs.each do |obj|
        tries = 0
        begin
          tries += 1
          do_one_archive(obj)
          @archived += 1
        rescue => e
          LyberCore::Log.error "Rolling back transaction due to: #{e.inspect}\n" << e.backtrace.join("\n") << "\n!!!!!!!!!!!!!!!!!!"
          if tries < 3 # Retry this druid up to 3 times
            LyberCore::Log.error "  Retrying archive operation in #{@retry_delay} seconds..."
            sleep @retry_delay
            retry
          end
          LyberCore::Log.error "  Too many retries.  Giving up on #{obj.inspect}"

          @errors += 1
          if @errors >= 3
            LyberCore::Log.fatal('Too many errors. Archiving halted')
            break
          end
        end
      end # druids.each
    end

    # @param [ArchiveCriteria] workflow_info contains paramaters on the workflow rows to archive
    def do_one_archive(workflow_info)
      LyberCore::Log.info "Archiving #{workflow_info.inspect}"
      copy_sql = <<-EOSQL
        insert into #{@workflow_archive_table} (
          #{wf_column_string},
          version
        )
        select
          #{wf_archive_column_string},
          #{workflow_info.version} as version
        from #{@workflow_table}
        where #{@workflow_table}.druid =    :druid
        and #{@workflow_table}.datastream = :datastream
      EOSQL

      delete_sql = "delete from #{@workflow_table} where druid = :druid and datastream = :datastream "

      if(workflow_info.repository)
        copy_sql += "and #{@workflow_table}.repository = :repository"
        delete_sql += 'and repository = :repository'
      else
        copy_sql += "and #{@workflow_table}.repository IS NULL"
        delete_sql += 'and repository IS NULL'
      end

      conn.transaction do
        conn.run Sequel::SQL::PlaceholderLiteralString.new(copy_sql, workflow_info.to_bind_hash)

        LyberCore::Log.debug '  Removing old workflow rows'

        conn.run Sequel::SQL::PlaceholderLiteralString.new(delete_sql, workflow_info.to_bind_hash)
      end
    end

    # Finds objects where all workflow steps are complete
    # @return [Array<Hash{String=>String}>] each hash returned has the following keys:
    #   {"REPOSITORY"=>"dor", "DRUID"=>"druid:345", "DATASTREAM"=>"googleScannedBookWF"}
    def find_completed_objects
      return to_enum(:find_completed_objects) unless block_given?

      completed_query = <<-EOSQL
       select distinct repository, datastream, druid
       from workflow w1
       where w1.status in ('completed', 'skipped')
       and not exists
       (
          select *
          from workflow w2
          where w1.repository = w2.repository
          and w1.datastream = w2.datastream
          and w1.druid = w2.druid
          and w2.status not in ('completed', 'skipped')
       )
      EOSQL

      conn.fetch(completed_query) do |row|
        yield row
      end
    end

    # @param [Array<Hash>] rows result from #find_completed_objects
    # @return [Array<ArchiveCriteria>] each result mapped to an ArchiveCriteria object
    def map_result_to_criteria(rows)
      rows.lazy.map do |r|
        begin
          ArchiveCriteria.new.setup_from_query(r)
        rescue => e
          LyberCore::Log.error("Skipping archiving of #{r['DRUID']}")
          LyberCore::Log.error("#{e.inspect}\n" + e.backtrace.join("\n"))
          nil
        end
      end.reject { |r| r.nil? }
    end

    # Does the work of finding completed objects and archiving the rows
    def archive
      objs = find_completed_objects

      if objs.none?
        LyberCore::Log.info 'Nothing to archive'
        exit true
      end
      LyberCore::Log.info "Found #{objs.size} completed workflows"
      archiving_criteria = map_result_to_criteria(objs)
      archive_rows(archiving_criteria)

      LyberCore::Log.info "DONE! Processed #{@archived.to_s} objects with #{@errors.to_s} errors" if @errors < 3
    end
  end
end
