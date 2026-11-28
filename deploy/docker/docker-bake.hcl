variable "PLATFORMS" {
  default = "linux/amd64"
}

variable "IMAGE" {
  default = "pangrova/pangrova:latest"
}

variable "SOURCE_COMMIT" {
  default = ""
}

group "default" {
  targets = ["pangrova"]
}

target "native-build" {
  context = "."
  dockerfile = "deploy/docker/Dockerfile.native-build"
  target = "native-build"
  platforms = split(",", PLATFORMS)
}

# Publish compiler and dependency layers without compiling the Pangrova JNI or Java sources.
target "native-production-dependencies" {
  inherits = ["native-build"]
  target = "builder"
  output = ["type=cacheonly"]
}

target "pangrova" {
  context = "."
  dockerfile = "deploy/docker/Dockerfile"
  contexts = {
    native_build = "target:native-build"
  }
  platforms = split(",", PLATFORMS)
  tags = [IMAGE]
  args = {
    SOURCE_COMMIT = SOURCE_COMMIT
  }
}
