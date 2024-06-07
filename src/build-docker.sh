#!/bin/bash
acr_id="movieacr.azurecr.io"
image_name="azf-movie-scrapping" # could be any image name
docker login "$acr_id" -u "movieacr" -p "n6JPCanenwbrTRnhktnvJiN6x+Nacn08lSzIObLsdm+ACRDNH9V6"
# docker-compose up
docker push "$acr_id/$image_name:latest"