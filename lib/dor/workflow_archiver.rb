require 'rest_client'
require 'confstruct'
require 'lyber_core'
require 'oci8'

module Dor

  # Holds the paramaters about the workflow rows that need to be deleted
  ArchiveCriteria = Struct.new(:repository, :druid, :datastream, :version) do
    # @param [Array<Hash>] List of objects returned from {WorkflowArchiver#find_completed_objects}.  It expects the following keys in the hash
    #  "REPOSITORY", "DRUID", "DATASTREAM".  Note they are all caps strings, not symbols
    def setup_from_query(row_hash)
      self.repository = row_hash["REPOSITORY"]
      self.druid = row_hash["DRUID"]
      self.datastream = row_hash["DATASTREAM"]
      set_current_version
      self
    end

    # Removes version from list of members, then picks out non nil members and builds a hash of column_name => column_value
    # @return [Hash] Maps column names (in ALL caps) to non-nil column values
    def to_bind_hash
      h = {}
      members.reject{|mem| mem =~ /version/}.each do |m|
        h[m.swapcase] = self.send(m) if(self.send(m))
      end
      h
    end

    def set_current_version
      begin
        self.version = RestClient.get WorkflowArchiver.config.dor_service_uri + "/dor/v1/objects/#{self.druid}/versions/current"
      rescue RestClient::InternalServerError => ise
        raise unless(ise.inspect =~ /Unable to find.*in fedora/)
        LyberCore::Log.warn "#{ise.inspect}"
        LyberCore::Log.warn "Moving workflow rows with version set to '1'"
        self.version = '1'
      end
    end
  end

  class WorkflowArchiver
    WF_COLUMNS = %w(ID DRUID DATASTREAM PROCESS STATUS ERROR_MSG ERROR_TXT DATETIME ATTEMPTS LIFECYCLE ELAPSED REPOSITORY NOTE PRIORITY LANE_ID)

    # These attributes mostly used for testing
    attr_reader :conn, :errors

    def self.config
      @@conf ||= Confstruct::Configuration.new
    end

    # Sets up logging and connects to the database.  By default it reads values from constants:
    #  WORKFLOW_DB_LOGIN, WORKFLOW_DB_PASSWORD, WORKFLOW_DB_URI, DOR_SERVICE_URI but can be overriden with the opts Hash
    # @param [Hash] opts Options to override database parameters
    # @option opts [String] :login ('WORKFLOW_DB_LOGIN') Database login id
    # @option opts [String] :password ('WORKFLOW_DB_PASSWORD') Database password
    # @option opts [String] :db_uri ('WORKFLOW_DB_URI') Database uri
    # @option opts [String] :wf_table ('workflow') Name of the active workflow table
    # @option opts [String] :wfa_table ('workflow_archive') Name of the workflow archive table
    # @option opts [String] :dor_service_uri ('DOR_SERVICE_URI') URI of the DOR Rest service
    # @option opts [Integer] :retry_delay (5) Number of seconds to sleep between retries of database operations
    def initialize(opts={})
      @login = (opts.include?(:login) ? opts[:login] : WorkflowArchiver.config.db_login)
      @password = (opts.include?(:password) ? opts[:password] : WorkflowArchiver.config.db_password)
      @db_uri = (opts.include?(:db_uri) ? opts[:db_uri] : WorkflowArchiver.config.db_uri)
      @dor_service_uri = (opts.include?(:dor_service_uri) ? opts[:dor_service_uri] : WorkflowArchiver.config.dor_service_uri)
      @workflow_table = (opts.include?(:wf_table) ? opts[:wf_table] : "workflow")
      @workflow_archive_table = (opts.include?(:wfa_table) ? opts[:wfa_table] : "workflow_archive")
      @retry_delay = (opts.include?(:retry_delay) ? opts[:retry_delay] : 5)

      # initialize some counters
      @errors = 0
      @archived = 0
    end

    def connect_to_db
      $odb_pool ||= OCI8::ConnectionPool.new(1, 5, 2, @login, @password, @db_uri)
      @conn = OCI8.new(@login, @password, $odb_pool)
      @conn.autocommit = false
    end

    def destroy_pool
      $odb_pool.destroy if($odb_pool)
    end

    def bind_and_exec_sql(sql, workflow_info)
      # LyberCore::Log.debug("Executing: #{sql}")
      cursor = @conn.parse(sql)

      workflow_info.to_bind_hash.each do |k, v|
        param = ":#{k}"
        #LyberCore::Log.debug("Setting: #{param} #{v}")
        cursor.bind_param(param, v)
      end

      num_rows = cursor.exec
      unless num_rows > 0
        raise "Expected more than 0 rows to be updated"
      end
    ensure
      cursor.close
    end

    # @return String The columns appended with comma and newline
    def wf_column_string
      WF_COLUMNS.inject('') { |str, col| str << col << ",\n"}
    end

    # @return String The columns prepended with 'w.' and appended with comma and newline
    def wf_archive_column_string
      WF_COLUMNS.inject('') { |str, col| str << 'w.' << col << ",\n"}
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
      connect_to_db
      archive_rows criteria
    ensure
      @conn.logoff if(@conn)
    end

    # Copies rows from the workflow table to the workflow_archive table, then deletes the rows from workflow
    # Both operations must complete, or they get rolled back
    # @param [Array<ArchiveCriteria>] objs List of objects returned from {#find_completed_objects} and mapped to an array of ArchiveCriteria objects.
    def archive_rows(objs)
      Array(objs).each do |obj|
        tries = 0
        begin
          tries += 1
          do_one_archive(obj)
          @archived += 1
        rescue => e
          LyberCore::Log.error "Rolling back transaction due to: #{e.inspect}\n" << e.backtrace.join("\n") << "\n!!!!!!!!!!!!!!!!!!"
          @conn.rollback

          # Retry this druid up to 3 times
          if tries < 3
            LyberCore::Log.error "  Retrying archive operation in #{@retry_delay.to_s} seconds..."
            sleep @retry_delay
            retry
          end
          LyberCore::Log.error "  Too many retries.  Giving up on #{obj.inspect}"

          @errors += 1
          if @errors >= 3
            LyberCore::Log.fatal("Too many errors. Archiving halted")
            break
          end
        end

      end # druids.each
    end

    # @param [ArchiveCriteria] workflow_info contains paramaters on the workflow rows to archive
    def do_one_archive(workflow_info)
      LyberCore::Log.info "Archiving #{workflow_info.inspect}"


      copy_sql =<<-EOSQL
        insert into #{@workflow_archive_table} (
          #{wf_column_string}
          VERSION
        )
        select
          #{wf_archive_column_string}
          #{workflow_info.version} as VERSION
        from #{@workflow_table} w
        where w.druid =    :DRUID
        and w.datastream = :DATASTREAM
      EOSQL

      delete_sql = "delete #{@workflow_table} where druid = :DRUID and datastream = :DATASTREAM "

      if(workflow_info.repository)
        copy_sql << "and w.repository = :REPOSITORY"
        delete_sql << "and repository = :REPOSITORY"
      else
        copy_sql << "and w.repository IS NULL"
        delete_sql << "and repository IS NULL"
      end

      bind_and_exec_sql(copy_sql, workflow_info)

      LyberCore::Log.debug "  Removing old workflow rows"
      bind_and_exec_sql(delete_sql, workflow_info)

      @conn.commit
    end

    # Finds objects where all workflow steps are complete
    # Returns an array of hashes, each hash having the following keys:
    # {"REPOSITORY"=>"dor", "DRUID"=>"druid:345", "DATASTREAM"=>"googleScannedBookWF"}
    def find_completed_objects
      completed_query =<<-EOSQL
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

      rows = []
      cursor = @conn.exec(completed_query)
      while r = cursor.fetch_hash
        rows << r
      end
      rows
    end

    # @param [Array<Hash>] rows result from #find_completed_objects
    # @return [Array<ArchiveCriteria>] each result mapped to an ArchiveCriteria object
    def map_result_to_criteria(rows)
      criteria = rows.map do |r|
        begin
          ArchiveCriteria.new.setup_from_query(r)
        rescue => e
          LyberCore::Log.error("Skipping archiving of #{r['DRUID']}")
          LyberCore::Log.error("#{e.inspect}\n" + e.backtrace.join("\n"))
          nil
        end
      end
      criteria.reject {|c| c.nil?}
    end

    def simple_sql_exec(sql)
      @conn.exec(sql)
    rescue Exception => e
      LyberCore::Log.warn "Ignoring error: #{e.message}\n  while trying to execute: " << sql
    end

    def with_indexing_disabled(&block)
      simple_sql_exec("drop index ds_wf_ar_bitmap_idx")
      simple_sql_exec("drop index repo_wf_ar_bitmap_idx")
      yield
    ensure
      simple_sql_exec("create bitmap index ds_wf_ar_bitmap_idx on workflow_archive (datastream)")
      simple_sql_exec("create bitmap index repo_wf_ar_bitmap_idx on workflow_archive (repository)")
    end

    # Does the work of finding completed objects and archiving the rows
    def archive
      connect_to_db
      objs = find_completed_objects

      if objs.size == 0
        LyberCore::Log.info "Nothing to archive"
        exit true
      end

      LyberCore::Log.info "Found #{objs.size.to_s} completed workflows"
      objs = objs.first(600) # FIXME: temporarily limit objs processed until we fix cron not to fire if job still running

      archiving_criteria = map_result_to_criteria(objs)
      with_indexing_disabled { archive_rows(archiving_criteria) }

      LyberCore::Log.info "DONE! Processed #{@archived.to_s} objects with #{@errors.to_s} errors" if(@errors < 3 )
    ensure
      @conn.logoff
      destroy_pool
    end

  end

end
