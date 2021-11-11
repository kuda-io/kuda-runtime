VERSION ?= 0.0.1

kudaRuntimeImage = kuda4bigo/kuda-runtime

docker-build:
	docker build -t $(kudaRuntimeImage) -f build/Dockerfile .
	docker push $(kudaRuntimeImage)