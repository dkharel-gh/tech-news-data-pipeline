# tech-news-data-pipeline
simple etl pipeline focusing on tech news data from hackernews, techcrunch and github trending (to start with)

Step 1: need a db container running.
    - postgres image can be pulled from dockerhub.
    - the etl service depends on running postgres db container.
    - the etl service needs a Dockerfile to build the image.
    - it also needs a python module that will run in the etl.

    - setup the etl/etl folder as the module
    - fixed the services in docker compose to run the db container and the etl-runner (from dockerfile).

Step 2: need a python script to download the data
    - for now let's just print helloworld
    - used the hn_test.py script to download 50 topics at once
    - made it display the polls and keep it running.

Step 3: need a python script to load the data to the db container.
    - 