#!/bin/bash
acr_id="$ACR_ID"
image_name="azf-movie-scrapping"
docker login "$acr_id" -u "$ACR_U" -p "$ACR_PASSWORD"
docker-compose up
docker push "$acr_id/$image_name:latest"
