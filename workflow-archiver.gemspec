# -*- encoding: utf-8 -*-
lib = File.expand_path('../lib/', __FILE__)
$:.unshift lib unless $:.include?(lib)
require 'dor/archiver_version'

Gem::Specification.new do |s|
  s.name        = "workflow-archiver"
  s.version     = Dor::ARCHIVER_VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Willy Mene"]
  s.email       = ["wmene@stanford.edu"]
  s.summary     = "Enables archiving of DOR workflows"
  s.description = "Can be used standalone or used as a library"
  s.executables = ["run_archiver"]

  s.required_rubygems_version = ">= 1.3.6"

  # Runtime dependencies

  s.add_dependency "lyber-core"
  s.add_dependency "rest-client"
  s.add_dependency "ruby-oci8"

  s.add_development_dependency "lyberteam-devel", ">=1.0.0"
  s.add_development_dependency "rspec"

  s.files        = Dir.glob("lib/**/*") + ['bin/run_archiver']
  s.bindir       = 'bin'
  s.require_path = 'lib'
end
