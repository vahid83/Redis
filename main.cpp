///========================================================
/// A very simple example of redis local server/client model
/// 
/// By: Vahid Noormofidi
///========================================================

#include <future>
#include <vector>
#include <iostream>
#include <mutex>

#include <string>
#include <climits>

#include <cpp_redis/cpp_redis>

constexpr int BASE_PORT = 9000;
using namespace std;
///---------------------
/// The below maps can be part of the Control class.
/// However, in a true distributed env. they must be accessible by all classes
/// Thus, they can be replacd by a distributed mapping

/// A map from <node_port> to it's status <online, capacity>
unordered_map< int, pair<bool, int>  > server_status;
/// One mutex per server, may be more if server and client support multi-threading
unordered_map< int, mutex > server_guard;
///---------------------

class Control
{
private:
	string server_path;	///< The path to the redis-server
	int NUM_OF_SERVERS;	///< Total number of servers to be init.
	int RL;				///< Reliability Level
	string IP;				///< IP address of redis
	
	static void p_put(int key, int value, int port)
	{
		/// Try to get the server/port lock
		lock_guard<mutex> guard(server_guard[port]);
		
		cout << "p=" << port << ", ";
		cpp_redis::client client;
		try
		{
			client.connect(IP, port);
			client.set(to_string(key), to_string(value));
			client.sync_commit();
			server_status[port].second--;
		}
		catch (const exception &e)
		{
			cerr << "Error in adding key, val: " << e.what() << endl;
			client.disconnect();
		}
	}

	static cpp_redis::reply p_get(int key, int port)
	{
		cpp_redis::client client;
		client.connect(IP, port);
		auto client_ret = client.get(to_string(key));
		client.sync_commit();
		auto ret = client_ret.get();
		return ret;
	}

public:
	Control() : 
		NUM_OF_SERVERS(-1), RL(1), server_path("./redis-server"), IP("127.0.0.1") {}
	Control(string path) : server_path(path) {}
	Control(string path, string ip): server_path(path), IP("127.0.0.1") {}

	/// Initialize the system. Must be called only once from one object
	void bringUP(int N, int capacity, int reliabilityLevel = 1)
	{
		if (N < reliabilityLevel)
		{
			cerr << "\n !!!!! ERROR !!!!! Reliability level " << reliabilityLevel << " cannot be achieved by " << N << " nodes... exiting...\n";
			exit(-1);
		}
		if (NUM_OF_SERVERS != -1)
		{
			cerr << "\n !!! WARNING !!! Servers' already initialized! Continuing with previous config.\n";
			return;
		}

		cout << "\n *** Bringing up " << N << " servers each with capacity " << capacity << " and with " << reliabilityLevel << " redundancy... ";
		NUM_OF_SERVERS = N;
		RL = reliabilityLevel;
		for (int i = 0; i < N; ++i)
		{
			server_status[BASE_PORT+i] = {true, capacity};
			string cmd = server_path + " --port " + to_string(BASE_PORT+i) + " > /dev/null &";
			int ret = system(cmd.c_str());
		}
		system("sleep 1");
		cout << "DONE!\n";
	}

	void shutdown()
	{
		cout << "\n *** Shutting down all the servers...\n";
		system("pkill redis-server");
	}

	int getNodeIndex(int key)
	{
		if (NUM_OF_SERVERS < 0)
		{
			cerr << "\n !!!!! ERROR !!!!! Please use bringUP function to initialize the system...\n";
			shutdown();
			exit(-2);
		}

		//hash<int> key_node(key);
		return hash<int>()(key) % NUM_OF_SERVERS;		
	}

	/// Put key,value asynchronously
	void put(int key, int value)
	{
		int node_port = getNodeIndex(key);

		cout << "\n *** Putting (key, value): " << key << ", " << value << " on the node(s) with port(s): ";

		/// check how many skipping has happened!
		int no_success = 0;
		
		/// For calling each put async.
		vector< future< void > > futures;
		for (int r = 0; r < RL; ++r)
		{
			auto np = BASE_PORT+((node_port + r) % NUM_OF_SERVERS);	///< e.g. 9000+p = 9000, 9001, 9002, etc.
			auto status = server_status[np];

			if (status.first == false)
			{
				cerr << "\n !!! WARNING !!! Server at port: " << np << " is offline. Skipping the server. Target reliability cannot be reached! \n";
				++no_success;
				continue;
			}
			if (status.second == 0)
			{
				cerr << "\n !!! WARNING !!! Server at port: " << np << " has reached its capacity. Skipping the server. Target reliability cannot be reached! \n";
				++no_success;
				continue;
			}
			/// Putting Async.	
			futures.emplace_back( async(launch::async, &Control::p_put, key, value, np) );
		}
	
		/// Check to see if any of the put command went through
		if (RL == no_success)
		{
			cerr << "\n !!!!! ERROR !!!!! Could not put (" << key << ", " << value << ") anywhere!\n" << endl;
			return;
		}

		for (auto& f : futures)
			f.get();		

		cout << "DONE!\n";
	}

	/// Can also be implemented as asynchronous similar to put()
	int get(int key)
	{
		int node_port = getNodeIndex(key);

		cout << "\n *** Getting value for key: " << key << " from the node at port: ";
		for (int r = 0; r < RL; ++r)
		{
			auto np = BASE_PORT+((node_port + r) % NUM_OF_SERVERS);
			auto status = server_status[np];

			if (status.first == false)
			{
				cerr << "\n !!! WARNING !!! Server at port: " << np << " is offline. Skipping the server! \n";
				continue;
			}
			
			cout << "p=" << np << ", ";
			auto ret = p_get(key, np);
			if (ret.ok() && ret.is_string())
			{
				cout << "Retrieved value is: " << stoi(ret.as_string()) << endl;
				return stoi(ret.as_string());
			}
		}
		
		cerr << "\n !!!!! ERROR !!!!! Could not find any value for the given key!\n";
		return INT_MIN;
	}

	void failNode(int port)
	{
		cout << "\n ##### Killing the server at port: " << port << endl;
		string cmd = "kill -9 $(ps -x | grep redis-server | grep "+to_string(port)+" | awk '{print $1}')";
		system(cmd.c_str());
		server_status[port].first = false;
	}

};

int main(int argc, char** argv)
{	
	/// Optionaly can change the server path
	string s_path("./redis-4.0.6/src/redis-server");
	string ip("127.0.0.1");
	if (argc > 1)
		s_path = argv[1];
	if (argc > 2)
		ip = argv[2];

	Control control(s_path, ip);	
	try
	{
		control.bringUP(5, 3, 2);
	}
	catch (const exception &e)
	{
		cerr << "Error during server init: " << e.what() << endl;
	}
	catch (...)
	{
		cerr << "\nUnknown exception!\n";
		control.shutdown();
	};

	/// Some tests...
	control.put(1,2);
	control.put(111,2);
	control.put(103,145);
	control.put(32,4);
	control.failNode(9001);
	control.put(11,2);
	control.put(64,2);
	/// The below cannot retrive the value
	cout << "Key 11= " <<  control.get(11) << endl;
	/// The below will fail due to wrong key
	cout << "Key 139= " <<  control.get(139) << endl;
	cout << "Key 32= " <<  control.get(32) << endl;
	cout << "Key 111= " <<  control.get(111) << endl;
	control.failNode(9003);
	cout << "Key 103= " <<  control.get(103) << endl;
	/// The cluster still has enough room, but the following fails
	control.put(1111,20);

	control.shutdown();

	return 0;
}
