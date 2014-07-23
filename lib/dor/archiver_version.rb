module Dor
  unless Dor.const_defined? :ARCHIVER_VERSION
    def self.archiver_version
      @archiver_version ||= File.read(File.join(File.dirname(__FILE__), '..', '..', 'VERSION')).chomp
    end

   ARCHIVER_VERSION = self.archiver_version
   end


end
