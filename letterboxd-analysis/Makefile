build:
	sh generate-image.sh

run:
	docker run --rm -v $(shell pwd)/data:/opt/spark-app/data --env-file .env --cpus="4" --memory="4g" letterboxd-analysis