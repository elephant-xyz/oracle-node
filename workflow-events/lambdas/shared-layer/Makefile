.PHONY: build-SharedLayer

build-SharedLayer:
	npm install
	mkdir -p "$(ARTIFACTS_DIR)/nodejs/node_modules/shared"
	npx esbuild src/*.ts \
		--bundle \
		--platform=node \
		--target=es2022 \
		--format=esm \
		--outdir="$(ARTIFACTS_DIR)/nodejs/node_modules/shared" \
		--out-extension:.js=.mjs \
		--external:@aws-sdk/* \
		--sourcemap
	cp src/package.json "$(ARTIFACTS_DIR)/nodejs/node_modules/shared/"
