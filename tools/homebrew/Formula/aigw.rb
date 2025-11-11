class Aigw < Formula
  desc "Envoy AI Gateway CLI"
  homepage "https://aigateway.envoyproxy.io/docs/cli/"
  license "Apache-2.0"

  version "0.4.0"
  checksums = {
    "darwin-arm64" => "00683ae0c9cdbab68b3d22e34d8e3a5a76327be3e203aadd132557d201ce1c50",
    "linux-arm64"  => "1ce163c923265e6da1244a186814508b5afc2422e0d5be13ac21ae6081f6c3ac",
    "linux-amd64"  => "c84d640eefddc04f8c96053ecfd7e6523fdb218e54aef144a5dac85064067b99",
  }

  OS_NAME = OS.mac? ? "darwin" : "linux"
  ARCH = Hardware::CPU.arm? ? "arm64" : "amd64"

  url "https://github.com/envoyproxy/ai-gateway/releases/download/v#{version}/aigw-#{OS_NAME}-#{ARCH}"
  sha256 checksums["#{OS_NAME}-#{ARCH}"]

  def install
    bin.install "aigw-#{self.class::OS_NAME}-#{self.class::ARCH}" => "aigw"
  end

  test do
    system "#{bin}/aigw", "version"
  end
end
