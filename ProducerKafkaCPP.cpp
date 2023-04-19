#pragma once

#include <string>
#include <iostream>
#include <pqxx/pqxx>
#include <sstream>
#include <vector>

#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>

#if _AIX
#include <unistd.h>
#endif

#include <librdkafka/rdkafkacpp.h>
#include <librdkafka/rdkafka.h>

static volatile sig_atomic_t run = 1;

static void sigterm(int sig) {
	run = 0;
}

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
public:
	void dr_cb(RdKafka::Message& message) {
		if (message.err())
			std::cerr << "% Message delivery failed: " << message.errstr()
			<< std::endl;
		//else
			//std::cout << "Hello \n";
	//	std::cerr << "% Message delivered to topic " << message.topic_name()
		//	<< " [" << message.partition() << "] at offset "
	//		<< message.offset() << std::endl;
	}
};



using namespace std;


void viewAllEntries(string connectionString){


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
			std::cout << "\nStudent ID: " << response[i][0] << "\nName: " << response[i][1] << "\nClass: " << response[i][2] << "\n----------------------------" << std::endl;
		}


		}
		catch (const std::exception& e)
		{
			std::cerr << e.what() << std::endl;
		}
		
	

}

string getData() {



		string data[5];

		string inp;
		string studentData ="";

		studentData.append("A");

		string messagePromits[] = { "Enter new Student Id : ", "Enter new Name : ", "Enter class : " };
		for (int i = 0; i < 3; i++) {
			
			cout << messagePromits[i] ;
			getline(cin >> ws, inp);
			studentData.append("-");
			studentData.append(inp);
			
		}
		cout << "Add Data Created : " << studentData << "\n" ;

		return studentData;
		
}


string getUpdate(string connectionString) {

	try {
		pqxx::connection connectionObject(connectionString.c_str());
		if (connectionObject.is_open()) {
			cout << "[Update] Opened database successfully: " << connectionObject.dbname() << endl;
		}
		else {
			cout << "[Update] Can't open database" << endl;
		}

		pqxx::work worker(connectionObject);

		pqxx::result response = worker.exec("SELECT * FROM student");

		for (size_t i = 0; i < response.size(); i++)
		{
			std::cout << "\nStudent ID: " << response[i][0] << "::Name: " << response[i][1] << "::Class: " << response[i][2] << std::endl;
		}

		string updateString = "";

			cout << "\nEnter ID to be Updated : ";
			
			string userID;
			getline(cin >> ws, userID);

			cout << "\nUpdate Menu \n1 --> Update Name \n2 --> Update Class \nEnter Command : ";
			updateString.append("U-");
			updateString.append(userID);
			updateString.append("-");
			int inp1;
			cin >> inp1;
			if (inp1 == 1) {
				updateString.append("updName-");
				cout << "\nEnter the new name : ";
			}
			else if (inp1 == 2) {
				updateString.append("updClass-");
				cout << "\nEnter the new class : ";
			}
			string newField;
			getline(cin >> ws, newField);
			updateString.append(newField);

		return updateString;
		
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

void sendToKafka(string dataLine) {
	std::string brokers = "localhost:9092";
	std::string topic = "studentData-topic";
	std::cout << topic << std::endl;

	// Create configuration object
	RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	std::cout << conf << std::endl;
	std::string errstr;

	// Set bootstrap broker(s) (default port 9092)

	if (conf->set("bootstrap.servers", brokers, errstr) !=
		RdKafka::Conf::CONF_OK) {
		std::cerr << errstr << std::endl;
		exit(1);
	}

	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);

	ExampleDeliveryReportCb ex_dr_cb;

	if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK) {
		//std::cout << "Hello ";
		std::cerr << errstr << std::endl;
		exit(1);
	}

	// Producer instance 
	RdKafka::Producer* producer = RdKafka::Producer::create(conf, errstr);
	if (!producer) {
		std::cerr << "Failed to create producer: " << errstr << std::endl;
		exit(1);
	}

	delete conf;

	string line = dataLine;

	RdKafka::ErrorCode err = producer->produce(
		topic,
		RdKafka::Topic::PARTITION_UA,
		RdKafka::Producer::RK_MSG_COPY,
		const_cast<char*>(line.c_str()), line.size(),
		NULL, 0,
		0,
		NULL,
		NULL);

	if (err != RdKafka::ERR_NO_ERROR) {
		std::cerr << "% Failed to produce to topic " << topic << ": "
			<< RdKafka::err2str(err) << std::endl;

	}
	else {
		//std::cerr << "% Enqueued message (" << line.size() << " bytes) "
		//	<< "for topic " << topic << std::endl;
	}

	//std::cerr << "% Flushing final messages..." << std::endl;
	producer->flush(10 * 1000 /* wait for max 10 seconds */);

	if (producer->outq_len() > 0)
		std::cerr << "% " << producer->outq_len()
		<< " message(s) were not delivered" << std::endl;

	delete producer;



}


int main()
{

	std::string connectionString = "host=localhost port=5432 dbname=mytestdb user=postgres password =admin";
    
    try
    {
        

		string commands = "\n1. - View all \n"
			"2. - Add  \n"
			"3. - Update \n"
			"4. - Delete \n"
			"5. - Exit the program \n";

		string promptInp = "\nEnter a Command: ";


		int cmd;

		bool running = true;

		cout << commands;

		while (running) {

			string kafkaMesasage = "";

			cout << promptInp;
			cin >> cmd;

			switch (cmd) {
			case 1:
				cout << "VIEW ALL\n";

				viewAllEntries(connectionString);


				break;
			case 2:
				cout << "ADD \n";

				kafkaMesasage = getData();
				sendToKafka(kafkaMesasage);
				break;

			case 3:
				cout << "UPDATE \n";
				kafkaMesasage = getUpdate(connectionString);
				sendToKafka(kafkaMesasage);
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
				cout << commands;
				break;
			}


		}
		cout << "\nEnd of Program.....";


        
    }
    catch (const std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }

    //system("pause");
}