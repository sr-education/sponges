# encoding: utf-8
require 'boson/runner'
require_relative 'sponges/configuration'
require_relative 'sponges/cpu_infos'
require_relative 'sponges/worker_builder'
require_relative 'sponges/supervisor'
require_relative 'sponges/runner'
require_relative 'sponges/cli'

module Sponges
  SIGNALS = [:INT, :QUIT, :TERM]

  def configure(&block)
    Sponges::Configuration.configure &block
  end
  module_function :configure
end
