FROM 218546966473.dkr.ecr.us-east-1.amazonaws.com/stitch-orchestrator:0.0.3

RUN mkdir /code/stream-closeio
WORKDIR /code/stream-closeio
ADD . /code/stream-closeio

RUN python3 setup.py install

CMD stitch-orchestrator sync \
  --streamer stream-closeio \
  --persister persist-stitch \
  --client-id $STITCH_CLIENT_ID \
  --connection-id $STITCH_CONNECTION_ID
