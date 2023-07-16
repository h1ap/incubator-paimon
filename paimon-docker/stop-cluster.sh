docker-compose -f $1 down -v
echo "stop paimon-docker env..."
rm -f ./data_mocker/log/*
echo "remove app log..."