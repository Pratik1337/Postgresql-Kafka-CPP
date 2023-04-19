#pragma once

#include <string>
#include <iostream>
#include <pqxx/pqxx>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <librdkafka/rdkafkacpp.h>

bool running = true;

static void stop(int sig) {
	running = false;
}

using namespace std;


void addEntry(string connectionString) {


	try {

		pqxx::connection connectionObject(connectionString.c_str());
		if (connectionObject.is_open()) {
			cout << "Opened database successfully: " << connectionObject.dbname() << endl;
		}
		else {
			cout << "Can't open database" << endl;
		}

		pqxx::work worker(connectionObject);

		string data[3];
		string arr1[] = { "Enter new Student Id : ", "Enter new Name : ", "Enter class : " };
		for (int i = 0; i < 3; i++) {

			cout << arr1[i];
			getline(cin >> ws, data[i]);

			//cin >> data[i];
		}

		string sql = "INSERT INTO student (id, name, class) VALUES ('" + data[0] + "','" + data[1] + "','" + data[2] + "');";
		worker.exec(sql);
		worker.commit();
		}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}



}


void addEntry(string connectionString, string id, string name, string studClass) {


	try {

		pqxx::connection connectionObject(connectionString.c_str());
		if (connectionObject.is_open()) {
			cout << "Opened database successfully: " << connectionObject.dbname() << endl;
		}
		else {
			cout << "Can't open database" << endl;
		}

		pqxx::work worker(connectionObject);


		string sql = "INSERT INTO student (id, name, class) VALUES ('" + id + "','" + name + "','" + studClass + "');";
		worker.exec(sql);
		worker.commit();
		cout << "Data Added\n";
		std::cout << id << "." << name << "." << studClass << " saved to Database!\n";

	}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}



}


void updateEntry(string connectionString, string id, string choice, string strUpdate) {

	try {
		pqxx::connection connectionObject(connectionString.c_str());
		if (connectionObject.is_open()) {
			cout << "Opened database successfully: " << connectionObject.dbname() << endl;
		}
		else {
			cout << "Can't open database" << endl;
		}

		pqxx::work worker(connectionObject);

		if (choice == "updName") {
			string sql = "UPDATE student set name = '" + strUpdate + "' where ID='" + id + "';";
			worker.exec(sql);
			worker.commit();
			cout << "\nUpdate Succeessfull!";
			cout << "\nUpdated Name to " << strUpdate << " for ID : " << id << "\n";
		}else if (choice == "updClass"){
			string sql = "UPDATE student set class = '" + strUpdate + "' where ID='" + id + "';";
			worker.exec(sql);
			worker.commit();
			cout << "\nUpdate Succeessfull!";
			cout << "\nUpdated Class to " << strUpdate << " for ID : " << id << "\n";
		}
	}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}


}



void deleteEntry(string connectionString) {

	try {
		pqxx::connection connectionObject(connectionString.c_str());
		if (connectionObject.is_open()) {
			cout << "Opened database successfully: " << connectionObject.dbname() << endl;
		}
		else {
			cout << "Can't open database" << endl;
		}

		pqxx::work worker(connectionObject);

		pqxx::result response = worker.exec("SELECT * FROM student");

		for (size_t i = 0; i < response.size(); i++)
		{
			std::cout << "\nStudent ID: " << response[i][0] << ",Name: " << response[i][1] << ",Class: " << response[i][2] << std::endl;
		}

		bool flg = true;

		while (flg)
		{
			cout << "\nPress 0 to Exit \nEnter ID to Delete : ";
			string delID = "";
			getline(cin >> ws, delID);

			if (delID == "0") {
				flg = false;
				continue;
			}

			response = worker.exec("DELETE from student where ID = '" + delID + "';");
			cout << "\nID : " << delID << " Deleted! \n";
		}
		worker.commit();
	}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}


}



class ExampleConsumeCb : public RdKafka::ConsumeCb {
public:
	std::string consume_cb1(RdKafka::Message& message, void* opaque) {
		
		string msg = "";
		
		switch (message.err()) {
		case RdKafka::ERR__TIMED_OUT:
			break;
		case RdKafka::ERR_NO_ERROR:
			//std::cout << "Received message: " << std::string(static_cast<const char*>(message.payload()), message.len()) << std::endl;
			msg = std::string(static_cast<const char*>(message.payload()));
			
			break;
		case RdKafka::ERR__PARTITION_EOF:
			std::cout << "Reached end of partition" << std::endl;
			break;
		case RdKafka::ERR__UNKNOWN_TOPIC:
		case RdKafka::ERR__UNKNOWN_PARTITION:
			std::cerr << "Consume failed: " << message.errstr() << std::endl;
			running = false;
			break;
		default:
			std::cerr << "Consume failed: " << message.errstr() << std::endl;
			running = false;
			break;
		}
		return msg;
	}


	void consume_cb(RdKafka::Message& message, void* opaque) {

		string msg = "";

		switch (message.err()) {
		case RdKafka::ERR__TIMED_OUT:
			break;
		case RdKafka::ERR_NO_ERROR:
			//std::cout << "Received message: " << std::string(static_cast<const char*>(message.payload()), message.len()) << std::endl;
			msg = std::string(static_cast<const char*>(message.payload()));
			
			break;
		case RdKafka::ERR__PARTITION_EOF:
			std::cout << "Reached end of partition" << std::endl;
			break;
		case RdKafka::ERR__UNKNOWN_TOPIC:
		case RdKafka::ERR__UNKNOWN_PARTITION:
			std::cerr << "Consume failed: " << message.errstr() << std::endl;
			running = false;
			break;
		default:
			std::cerr << "Consume failed: " << message.errstr() << std::endl;
			running = false;
			break;
		}
	}
};






int main()
{

	std::string connectionString = "host=localhost port=5432 dbname=mytestdb user=postgres password =admin";


	std::string brokers = "localhost:9092";
	//std::string topic = "studentData-topic";
	std::string topic = "studentData-topic";
	std::string group_id = "my-group";
	int32_t partition = RdKafka::Topic::PARTITION_UA;

	ExampleConsumeCb consume_cb;
	RdKafka::Conf* conf = nullptr;
	RdKafka::Conf* tconf = nullptr;
	RdKafka::KafkaConsumer* consumer = nullptr;
	std::string errstr;

	// Set up Kafka configuration
	conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	if (conf == nullptr) {
		std::cerr << "Failed to create Kafka configuration" << std::endl;
		return EXIT_FAILURE;
	}

	// Set up broker list
	if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "Failed to set broker list: " << errstr << std::endl;
		delete conf;
		return EXIT_FAILURE;
	}

	// Set up group ID
	if (conf->set("group.id", group_id, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "Failed to set group ID: " << errstr << std::endl;
		delete conf;
		return EXIT_FAILURE;
	}

	// Set up consume callback
	if (conf->set("consume_cb", &consume_cb, errstr) != RdKafka::Conf::CONF_OK) {
		std::cerr << "Failed to set consume callback: " << errstr << std::endl;
		delete conf;
		return EXIT_FAILURE;
	}

	// Set up topic configuration
	tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
	if (tconf == nullptr) {
		std::cerr << "Failed to create Kafka topic configuration" << std::endl;
		delete conf;
		return EXIT_FAILURE;
	}

	// Create Kafka consumer
	consumer = RdKafka::KafkaConsumer::create(conf, errstr);
	if (consumer == nullptr) {
		std::cerr << "Failed to create Kafka consumer: " << errstr << std::endl;
		delete conf;
		delete tconf;
		return EXIT_FAILURE;
	}


	// Subscribe to topic
	std::vector<std::string> topics = { topic };
	RdKafka::ErrorCode err = consumer->subscribe(topics);
	if (err != RdKafka::ERR_NO_ERROR) {
		std::cerr << "Failed to subscribe to topic: " << RdKafka::err2str(err) << std::endl;
		delete conf;
		delete tconf;
		delete consumer;
		return EXIT_FAILURE;
	}

	// Set up signal handler
	signal(SIGINT, stop);

	// Consume messages
	while (running) {
		RdKafka::Message* message = consumer->consume(1000);
		string msg = consume_cb.consume_cb1(*message, nullptr);
		if (msg != "") {
			//std::cout << msg << "\n";

			stringstream ss(msg);
			string word;

			vector<string> splitWords;

			while (!ss.eof()) {
				getline(ss, word, '-');
				splitWords.push_back(word);
			}

			string cmd = splitWords[0];

			if (cmd == "A") {
				addEntry(connectionString, splitWords[1], splitWords[2], splitWords[3]);
			}
			else if (cmd == "U") {
				updateEntry(connectionString, splitWords[1], splitWords[2], splitWords[3]);
			}
		

		}
		
		delete message;
	}

	




	
	/*

	try
	{


		int cmd;

		bool running = true;

		while (running) {

			switch (cmd) {
			
			case 2:
				cout << "Add new entry command recieved \n";

				//addEntry(connectionString);

				

				addData(connectionString, inp_id, inp_name, inp_class);
				break;

			case 3:
				cout << "UPDATE \n";
				updateEntry(connectionString);
				break;
			case 4:
				cout << "DELETE \n";
				deleteEntry(connectionString);
				break;
			case 5:
				cout << "EXIT \n";
				running = false;
				break;
			default:
				cout << "INVALID COMMAND! \n";
				break;
			}


		}
		cout << "\nEnd of Program.....";



	}
	catch (const std::exception& e)
	{
		std::cerr << e.what() << std::endl;
	}
	*/

	// Unsubscribe from topic
	consumer->unsubscribe();

	// Cleanup
	delete consumer;
	delete conf;
	delete tconf;

	return EXIT_SUCCESS;



	//system("pause");
}