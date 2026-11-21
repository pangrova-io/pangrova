# Included in Velox's project(Arrow), before it declares arrow_ep. The pinned
# ExternalProject does not export a patch-step target by default.
set_property(DIRECTORY APPEND PROPERTY EP_STEP_TARGETS patch)
