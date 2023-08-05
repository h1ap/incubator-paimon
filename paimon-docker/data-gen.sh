rm -rf ./data_mocker/log/*.log
echo "remove app log..."

system=`uname`
if [ "$system" == "Darwin" ]; then
  gsed -i "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}/$(gdate +%Y-%m-%d)/g" ./data_mocker/application.yml
else
  sed -i "s/[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}/$(date +%Y-%m-%d)/g" ./data_mocker/application.yml
fi

docker_compose_file=$1

#echo "start paimon-docker: $docker_compose_file env..."
# docker-compose -f $docker_compose_file up -d
#sleep 5s

config_path='./data_mocker/application.yml'

for i in {1..1000} ; do
    if [ $i != 1 ]; then
        echo "exec datagen..."
        docker-compose -f "$docker_compose_file" start datagen
    fi
    datagen_status=$(docker-compose -f "$docker_compose_file" ps -a datagen | awk 'NR==2')
    echo "生成数据，日期：$date_str"
    echo "$datagen_status"
    while [[ $datagen_status != *"Exited"* ]]; do
      datagen_status=$(docker-compose -f "$docker_compose_file" ps -a datagen | awk 'NR==2')
      echo "$datagen_status"
      echo "wait datagen..."
      sleep 2
    done
    echo "生成完毕，日期：$date_str"
    sleep 2
    mv ./data_mocker/log/app.log ./data_mocker/log/app-$date_str.log
    if [ "$system" == "Darwin" ]; then
      gsed -i "s/mock.date: \"[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\"/mock.date: \"$(gdate -d +"$i days ago" +%Y-%m-%d)\"/g" ./data_mocker/application.yml
    else
      sed -i "s/mock.date: \"[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\"/mock.date: \"$(date -d +"$i days ago" +%Y-%m-%d)\"/g" ./data_mocker/application.yml
    fi
done