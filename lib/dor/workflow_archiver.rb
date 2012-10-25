require 'rest_client'

module Dor

  class WorkflowArchiver
    # These attributes mostly used for testing
    attr_reader :conn, :errors

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
      LyberCore::Log.set_logfile("#{ROBOT_ROOT}/log/workflow_archiver.log")
      LyberCore::Log.set_level(1)

      @login = (opts.include?(:login) ? opts[:login] : WORKFLOW_DB_LOGIN)
      @password = (opts.include?(:password) ? opts[:password] : WORKFLOW_DB_PASSWORD)
      @db_uri = (opts.include?(:db_uri) ? opts[:db_uri] : WORKFLOW_DB_URI)
      @dor_service_uri = (opts.include?(:dor_service_uri) ? opts[:dor_service_uri] : DOR_SERVICE_URI)
      @workflow_table = (opts.include?(:wf_table) ? opts[:wf_table] : "workflow")
      @workflow_archive_table = (opts.include?(:wfa_table) ? opts[:wfa_table] : "workflow_archive")
      @retry_delay = (opts.include?(:retry_delay) ? opts[:retry_delay] : 5)
    end

    def connect_to_db
      @conn = OCI8.new(@login, @password, @db_uri)
      @conn.autocommit = false
    end

    def bind_and_exec_sql(sql, obj)
      # LyberCore::Log.debug("Executing: #{sql}")
      cursor = @conn.parse(sql)
      obj.each do |k, v|
        param = ":#{k}"
        #LyberCore::Log.debug("Setting: #{param} #{v}")
        cursor.bind_param(param, v) if(v)
      end

      num_rows = cursor.exec
      unless num_rows > 0
        raise "Expected more than 0 rows to be updated"
      end
    ensure
      cursor.close
    end

    # Copies rows from the workflow table to the workflow_archive table, then deletes the rows from workflow
    # Both operations must complete, or they get rolled back
    # @param [Array<Hash>] List of objects returned from {#find_completed_objects}.  It expects the following keys in the hash
    #  "REPOSITORY", "DRUID", "DATASTREAM".  Note they are all caps strings, not symbols
    # TODO figure out what version to insert
    def archive_rows(objs)
      objs.each do |obj|
        tries = 0
        begin
          tries += 1
          LyberCore::Log.info "Archiving #{obj.inspect}"

          begin
            version = get_latest_version(obj["DRUID"])
          rescue RestClient::InternalServerError => ise
            raise unless(ise.inspect =~ /Unable to find.*in fedora/)
            LyberCore::Log.warn "#{ise.inspect}"
            LyberCore::Log.warn "Moving workflow rows with version set to '1'"
            version = '1'
          end
          copy_sql =<<-EOSQL
            insert into #{@workflow_archive_table} (
              ID,
              DRUID,
              DATASTREAM,
              PROCESS,
              STATUS,
              ERROR_MSG,
              ERROR_TXT,
              DATETIME,
              ATTEMPTS,
              LIFECYCLE,
              ELAPSED,
              REPOSITORY,
              NOTE,
              VERSION
            )
            select
              w.ID,
              w.DRUID,
              w.DATASTREAM,
              w.PROCESS,
              w.STATUS,
              w.ERROR_MSG,
              w.ERROR_TXT,
              w.DATETIME,
              w.ATTEMPTS,
              w.LIFECYCLE,
              w.ELAPSED,
              w.NOTE,
              w.REPOSITORY,
              #{version} as VERSION
            from #{@workflow_table} w
            where w.druid =    :DRUID
            and w.datastream = :DATASTREAM
          EOSQL

          delete_sql = "delete #{@workflow_table} where druid = :DRUID and datastream = :DATASTREAM "

          if(obj["REPOSITORY"])
            copy_sql << "and w.repository = :REPOSITORY"
            delete_sql << "and repository = :REPOSITORY"
          else
            copy_sql << "and w.repository IS NULL"
            delete_sql << "and repository IS NULL"
          end

          bind_and_exec_sql(copy_sql, obj)

          LyberCore::Log.debug "  Removing old workflow rows"
          bind_and_exec_sql(delete_sql, obj)

          @conn.commit
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

    def get_latest_version(druid)
      RestClient.get @dor_service_uri + "/dor/objects/#{druid}/versions/current"
    end

    # Finds objects where all workflow steps are complete
    # Returns an array of hashes, each hash having the following keys:
    # {"REPOSITORY"=>"dor", "DRUID"=>"druid:345", "DATASTREAM"=>"googleScannedBookWF"}
    def find_completed_objects
      completed_query =<<-EOSQL
       select distinct repository, datastream, druid
       from workflow w1
       where w1.status = 'completed'
       and not exists
       (
          select *
          from workflow w2
          where w1.repository = w2.repository
          and w1.datastream = w2.datastream
          and w1.druid = w2.druid
          and w2.status != 'completed'
       )
      EOSQL

      rows = []
      cursor = @conn.exec(completed_query)
      while r = cursor.fetch_hash
        rows << r
      end
      rows
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
      objs = find_completed_objects

      if objs.size == 0
        LyberCore::Log.info "Nothing to archive"
        exit true
      end

      LyberCore::Log.info "Found #{objs.size.to_s} completed workflows"

      @errors = 0
      @archived = 0
      with_indexing_disabled { archive_rows(objs) }

      LyberCore::Log.info "DONE! Processed #{@archived.to_s} objects with #{@errors.to_s} errors" if(@errors < 3 )
    ensure
      @conn.logoff
    end

  end

end
