FROM puckel/docker-airflow:1.10.9

RUN pip install boto3

ENTRYPOINT ["/entrypoint.sh"]
CMD ["webserver"]

