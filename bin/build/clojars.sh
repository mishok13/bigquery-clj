#!/bin/sh

set -e
set -x

# Deploy the SNAPSHOT version to Clojars. Additional check is needed
# to ensure that we don't deploy release versions accidentally (we
# shouldn't have them in master first of all, but that's a whole
# different story)

# Check that we're not building PR and we're on master branch
test $TRAVIS_PULL_REQUEST == "false"
test $TRAVIS_BRANCH == "master"

# Deploy a snapshot
lein2 deploy snapshots
