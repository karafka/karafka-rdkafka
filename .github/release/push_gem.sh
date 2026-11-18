#!/usr/bin/env bash
# Signs a gem with Sigstore and pushes it with the attestation. Exits before the push when
# signing fails, so no gem is published without its attestation.
set -euo pipefail

gem_file="$1"

gem install sigstore-cli -v 0.2.3 --no-document
gem install json -v 2.21.2 --no-document

# sigstore-cli does not work with json 3.x, the default of Ruby 4.0
ruby -e 'gem "json", "2.21.2"; load Gem.bin_path("sigstore-cli", "sigstore-cli")' \
  sign "$gem_file" --bundle "$gem_file.sigstore.json"

gem push "$gem_file" --attestation "$gem_file.sigstore.json"
