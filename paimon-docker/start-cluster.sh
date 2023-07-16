rm -rf ./data_mocker/log/*.log
echo "remove app log..."

date_str=$(date +%Y-%m-%d)
gsed -i "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}/$date_str/g" ./data_mocker/application.yml

docker_compose_file=$1

echo "start paimon-docker: $docker_compose_file env..."
docker-compose -f $docker_compose_file up -d
sleep 5s

config_path='./data_mocker/application.yml'

for i in {1..6} ; do
    if [ $i != 1 ]; then
        echo "exec datagen..."
        docker-compose -f $docker_compose_file start datagen
    fi
    datagen_status=$(docker-compose -f $docker_compose_file ps -a datagen | awk 'NR==2')
    echo "生成数据，日期：$date_str"
    while [[ $datagen_status != *"Exited"* ]]; do
      datagen_status=$(docker-compose -f $docker_compose_file ps -a datagen | awk 'NR==2')
      echo "wait datagen..."
      sleep 1s
    done
    echo "生成完毕，日期：$date_str"
    sleep 2s
    mv ./data_mocker/log/app.log ./data_mocker/log/app-$date_str.log
    date_str=$(date -d $i+" days ago" +%Y-%m-%d)
    gsed -i "s/mock.date: \"[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\"/mock.date: \"$date_str\"/g" ./data_mocker/application.yml
done

docker-compose -f $docker_compose_file exec datanode1 bash /opt/copy_log.sh
echo "copy app.log to hdfs..."