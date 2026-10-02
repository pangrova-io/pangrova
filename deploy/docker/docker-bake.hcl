variable "PLATFORMS" {
  default = "linux/amd64"
}

variable "IMAGE" {
  default = "pangrova/pangrova:latest"
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

target "pangrova" {
  context = "."
  dockerfile = "deploy/docker/Dockerfile"
  contexts = {
    native_build = "target:native-build"
  }
  platforms = split(",", PLATFORMS)
  tags = [IMAGE]
}
