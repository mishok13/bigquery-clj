#!/bin/sh

# Deploy the SNAPSHOT version to Clojars. Additional check is needed
# to ensure that we don't deploy release versions accidentally (we
# shouldn't have them in master first of all, but that's a whole
# different story)

lein2 deploy snapshots
