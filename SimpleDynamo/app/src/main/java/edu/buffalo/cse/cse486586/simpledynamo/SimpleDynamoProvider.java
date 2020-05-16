package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.preference.PreferenceManager;
import android.telephony.TelephonyManager;
import android.util.Log;

import static android.content.Context.MODE_PRIVATE;

public class SimpleDynamoProvider extends ContentProvider {

	static final int SERVER_PORT = 10000;
	private static final String KEY_FIELD = "key";
	private static final String VALUE_FIELD = "value";

	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	static final String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";

	String[] nodeOrder = {"5562", "5556", "5554", "5558", "5560"};
	ArrayList<String> remotePorts = new ArrayList<String>(5);
	Hashtable<BigInteger, String> hashDict = new Hashtable<BigInteger, String>();
	HashMap<String, Object[]> localDB = new HashMap<String, Object[]>();

	public SimpleDynamoProvider(){
		remotePorts.add(REMOTE_PORT0);
		remotePorts.add(REMOTE_PORT1);
		remotePorts.add(REMOTE_PORT2);
		remotePorts.add(REMOTE_PORT3);
		remotePorts.add(REMOTE_PORT4);
	}

	TreeSet<BigInteger> aliveNodes = new TreeSet<BigInteger>();

	BigInteger myHash;
	BigInteger succ;
	BigInteger prev;

	boolean recovery;


	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub

		ArrayList<String> remotePorts = findRemotePorts(selection);
		for (String remotePort : remotePorts) {
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(remotePort));
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(new Object[]{"DELETE", selection});
				out.flush();

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return 0;
	}

//		Object[] returnedValue = localDB.remove(selection);
//		try {
//			if (returnedValue == null) {
//				String remotePort = findRemotePort(selection);
//				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
//						Integer.parseInt(remotePort));
//				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//				out.writeObject(new Object[]{"DELETE", selection});
//				out.flush();
//			}
//		} catch (Exception e){
//			e.printStackTrace();
//		}
//		return 0;
//	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	public void replicate(BigInteger keyHash, Object[] values) {

		ArrayList<BigInteger> tempHash = new ArrayList<BigInteger>(aliveNodes);
		int index = 0;
		for (int i = 0; i < tempHash.size(); i++) {
			if (keyHash.compareTo(tempHash.get(i)) < 0) {
				index = i;
				break;
			}
		}

		new ClientTask().execute("REPLICATE", hashDict.get(tempHash.get((index + 1) % tempHash.size())), values, "1");
		new ClientTask().execute("REPLICATE", hashDict.get(tempHash.get((index + 2) % tempHash.size())), values, "2");

	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
		String key = (String) values.get(KEY_FIELD);
		String value = (String) values.get(VALUE_FIELD);

		Date currentTime = Calendar.getInstance().getTime();

		try {
			BigInteger keyHash = new BigInteger(genHash(key), 16);
//			insert(keyHash, new Object[]{key, value, currentTime});

			Object[] valueToInsert = new Object[]{value, String.valueOf(0), currentTime};
			replicate(keyHash, new Object[]{key, value, currentTime});

			Log.d("INSERT", keyHash + "--KEY--" + key);
			if ((prev.compareTo(myHash) > 0) || (prev.compareTo(myHash) == 0)){
				if ((keyHash.compareTo(prev) > 0) || (keyHash.compareTo(myHash)<0)){
					localDB.put(key, valueToInsert);
				}
				else {
					String remotePort = findRemotePort(key);
					new ClientTask().execute("INSERT", remotePort, key, valueToInsert);
				}
			}
			else if ((keyHash.compareTo(prev) > 0) & (keyHash.compareTo(myHash) < 0)) {
				localDB.put(key, valueToInsert);
			} else {
				String remotePort = findRemotePort(key);
				new ClientTask().execute("INSERT", remotePort, key, valueToInsert);
			}

		} catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		} catch (Exception e){
			e.printStackTrace();
		}
		return null;
	}

	public void nodeJoin(String portNumber, String currPort) {
		try {
			BigInteger nodeHash = new BigInteger(genHash(portNumber), 16);
			Log.d("HASH", portNumber + "-" + nodeHash);
			myHash = nodeHash;

//			aliveNodes.add(nodeHash);
			for (int i=0; i < nodeOrder.length; i++){
				BigInteger tempHash = new BigInteger(genHash(nodeOrder[i]), 16);
				aliveNodes.add(tempHash);
				hashDict.put(tempHash, String.valueOf(Integer.valueOf(nodeOrder[i])*2));
			}

			System.out.println(aliveNodes);
			updateNeighbours();

//			new ClientTask().execute("JOIN", REMOTE_PORT0, nodeHash);


		} catch (NoSuchAlgorithmException e) {
			Log.e("NodeJoin error", "NoSuchAlgorithmException");
		}
	}

//	public void dataRecovery(String remotePort, String identifier) {
//
////		for (int i = 0; i < remotePort.length; i++) {
//		try {
//			Object[] msg = new Object[]{"RECOVERY"};
//			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
//			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
//			out.writeObject(msg);
//			out.flush();
//
//			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
//			HashMap<String, Object[]> data = (HashMap<String, Object[]>) in.readObject();
//
//			for (Map.Entry<String, Object[]> entry: data.entrySet()) {
//				Object[] value = entry.getValue();
//
//				if (value[1].equals(identifier)) {
////						if (localDB.containsKey(entry.getKey())) {
////							if (((Date) localDB.get(entry.getKey())[2]).compareTo((Date) value[2]) < 0) {
////								localDB.put(entry.getKey(), entry.getValue());
////								continue;
////							}
////						}
//					localDB.put(entry.getKey(), entry.getValue());
//				}
//			}
//
//			} catch (Exception e) {
//			e.printStackTrace();
//		}
////		}
//	}

	public int getModulus(int number, int size){
		number = (number) % size;
		if (number < 0) number += size;
		return number;
	}

	public void recover(Object[] msgToSend){
		try {
			String remotePort = (String) msgToSend[1];
			String identifier = (String) msgToSend[2];
			String replace = (String) msgToSend[3];

			Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
			ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
			Log.d("RECOVERY", msgToSend[0] + "-" + msgToSend[1]);
			out.writeObject(msgToSend);
			out.flush();

			ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
			Object[] data = (Object[]) in.readObject();
			HashMap<String, Object[]> recoveryData = (HashMap<String, Object[]>) data[0];

			for (Map.Entry<String, Object[]> entry : recoveryData.entrySet()) {
				Object[] value = entry.getValue();

				if (value[1].equals(identifier)) {
					value[1] = replace;
					localDB.put(entry.getKey(), value);
				}
			}

		}catch (Exception e){
			e.printStackTrace();
		}
	}

	public void recoveryMode(){
		Log.d("RECOVERY MODE", "recovering data");
		ArrayList<BigInteger> tempHash = new ArrayList<BigInteger>(aliveNodes);
		int i;
		for (i=0; i<tempHash.size(); i++){
			if (myHash.compareTo(tempHash.get(i)) == 0){
				break;
			}
		}

		new ClientTask().execute("RECOVERY", hashDict.get(tempHash.get(getModulus(i + 1, tempHash.size()))), "1", "0");
		new ClientTask().execute("RECOVERY", hashDict.get(tempHash.get(getModulus(i - 1, tempHash.size()))), "0", "1");
		new ClientTask().execute("RECOVERY", hashDict.get(tempHash.get(getModulus(i - 2, tempHash.size()))), "0", "2");

		try {
			Log.d("RECOVERY", "sleeping");
			Thread.sleep(1000);
		} catch (InterruptedException e){
			e.printStackTrace();
		}
//		recover(new Object[]{"RECOVERY", hashDict.get(tempHash.get(getModulus(i + 1, tempHash.size()))), "1", "0"});
//		recover(new Object[]{"RECOVERY", hashDict.get(tempHash.get(getModulus(i - 1, tempHash.size()))), "0", "1"});
//		recover(new Object[]{"RECOVERY", hashDict.get(tempHash.get(getModulus(i - 2, tempHash.size()))), "0", "2"});
		Log.d("RECOVERY MODE", "Completed");

////		String[] remotePorts = new String[]{hashDict.get(tempHash.get((i + 1) % tempHash.size())), hashDict.get(tempHash.get((i + 2) % tempHash.size()))};
//		dataRecovery(hashDict.get(tempHash.get((i + 1) % tempHash.size())), "0");
//
////		remotePorts = new String[]{hashDict.get(tempHash.get((i - 1) % tempHash.size())), hashDict.get(tempHash.get((i - 2) % tempHash.size()))};
//		dataRecovery(hashDict.get(tempHash.get((i - 1) % tempHash.size())), "1");
//		dataRecovery(hashDict.get(tempHash.get((i - 2) % tempHash.size())), "2");
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String currPort = String.valueOf((Integer.parseInt(portStr)*2));
		Log.d("MAIN", portStr);
		nodeJoin(portStr, currPort);


//		boolean isAlive = true;
//		SharedPreferences preferences = PreferenceManager.getDefaultSharedPreferences(getContext());
//		isAlive = preferences.getBoolean("isAlive", false);

//		if (isAlive) {
//		} else {
//			SharedPreferences.Editor editor = preferences.edit();
//			editor.putBoolean("isAlive", isAlive); // value to store
//			editor.commit();
//		}
		try{

			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			Object[] serverPort = new Object[] {serverSocket, currPort};
			Log.d("Connecting to Server", "");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverPort);
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		recoveryMode();


		return false;
	}

	public String findRemotePort(String key){
		String remotePort = null;
		try {
			BigInteger keyHash = new BigInteger(genHash(key), 16);
			ArrayList<BigInteger> tempHash = new ArrayList<BigInteger>(aliveNodes);

			remotePort = hashDict.get(tempHash.get(0));
			for (int i = 0; i < tempHash.size(); i++) {
				if (keyHash.compareTo(tempHash.get(i)) < 0) {
					remotePort = hashDict.get(tempHash.get(i));
					break;
				}
			}

		} catch (NoSuchAlgorithmException e){
			e.printStackTrace();
		}

		return remotePort;
	}

	public ArrayList<String> findRemotePorts(String key){
		ArrayList<String> remotePorts = new ArrayList<String>();
		try {
			BigInteger keyHash = new BigInteger(genHash(key), 16);
			ArrayList<BigInteger> tempHash = new ArrayList<BigInteger>(aliveNodes);

			int index = 0;
			for (int i = 0; i < tempHash.size(); i++) {
				if (keyHash.compareTo(tempHash.get(i)) < 0) {
					index = i;
					break;
				}
			}

			remotePorts.add(hashDict.get(tempHash.get(getModulus(index, tempHash.size()))));
			remotePorts.add(hashDict.get(tempHash.get(getModulus(index+1, tempHash.size()))));
			remotePorts.add(hashDict.get(tempHash.get(getModulus(index+2, tempHash.size()))));

		}catch (Exception e){
			e.printStackTrace();
		}

		return remotePorts;
	}

	public Object[] findQuery(String key){
		ArrayList<String> remotePorts = findRemotePorts(key);
		HashMap<String, Object[]> tempDB = new HashMap<String, Object[]>();

		for (String remotePort: remotePorts) {
			Log.d("QUERYING", "remote ports and replication--" + remotePort);
			try {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(remotePort));
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(new Object[]{"QUERY", key});
				out.flush();

				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				Object[] value = (Object[]) in.readObject();
				if (tempDB.containsKey(key)){
					if ((((Date) tempDB.get(key)[2]).compareTo((Date) value[2])) < 0){
						tempDB.put(key, value);
						continue;
					}
				}
				tempDB.put(key, value);

			} catch (NullPointerException e){
				Log.e("NULL pointer exception", remotePort);
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}
		return tempDB.get(key);
	}


	public Set<String> globalQuery(){
		HashMap<String, Object[]> globalDB = new HashMap<String, Object[]>();
		for(int i = 0; i <=remotePorts.size()-1; i++) {
			try {
				// send happens here
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(remotePorts.get(i)));
				Log.d("Global Query", remotePorts.get(i));
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(new Object[]{"QUERY*"});
				out.flush();

				ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
				Object[] data = (Object[]) in.readObject();
				globalDB.putAll((HashMap<String, Object[]>) data[0]);
				socket.close();

			} catch (UnknownHostException e) {
				Log.e(TAG, "ClientTask UnknownHostException");
			} catch (IOException e) {
				Log.e(TAG, "ClientTask socket IOException");
			} catch (Exception e){
				e.printStackTrace();
			}
		}

		HashMap<String, Object[]> finalDB = new HashMap<String, Object[]>();

		for (Map.Entry<String, Object[]> entry: globalDB.entrySet()){
			if (finalDB.containsKey(entry.getKey())){
				if (((Date) finalDB.get(entry.getKey())[2]).compareTo((Date) entry.getValue()[2]) < 0){
					finalDB.put(entry.getKey(), entry.getValue());
					continue;
				}
			}
			finalDB.put(entry.getKey(), entry.getValue());
		}

		return finalDB.keySet();
	}

	@Override
	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
						String sortOrder) {
		// TODO Auto-generated method stub
		ArrayList<String> keysToQuery = new ArrayList<String>();

		if (selection.equals("*")) {
			keysToQuery = new ArrayList<String>(globalQuery());
		}
		else if (selection.equals("@")){
			keysToQuery = new ArrayList<String>(localDB.keySet());
		} else{
			keysToQuery.add(selection);
		}
		Log.d("QUERY", selection + "-" +keysToQuery.size());

		MatrixCursor cursor = new MatrixCursor(new String[]{"key", "value"});

		for (int i =0; i < keysToQuery.size(); i++) {
//			Object[] value = localDB.get(keysToQuery.get(i));

//			if ((value == null) || (!selection.equals("*") && !selection.equals("@") && !value[1].equals("0"))) {

			Object[] value = findQuery(keysToQuery.get(i));
//			}
			Log.d("QUERY", keysToQuery.get(i) + "--" + value[0] + "--" + value[1]);
			cursor.addRow(new Object[]{keysToQuery.get(i), value[0]});

		}
		return cursor;
	}


	@Override
	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}


	public void updateNeighbours(){

		try {
			ArrayList<BigInteger> tempHash = new ArrayList<BigInteger>(aliveNodes);
			Log.d("Number of Nodes", "" + tempHash.size());
			Log.d("My Hash", ""+myHash);
//            prev = tempHash.get(0);
//            succ = tempHash.get(0);
			for (int i = 0; i < tempHash.size(); i++) {
				if (tempHash.get(i).equals(myHash)) {
					int prev_idx = i - 1;
					int succ_idx = i + 1;

					if (prev_idx < 0){
						prev_idx = tempHash.size() - 1;
					}

					if (succ_idx == tempHash.size()){
						succ_idx = 0;
					}
					prev = tempHash.get(prev_idx);
					succ = tempHash.get(succ_idx);
				}

			}

		}catch (Exception e){
			Log.d("NEIGHBORS", "");
			System.out.println(aliveNodes);
			e.printStackTrace();
		}
	}

	private class ServerTask extends AsyncTask<Object, Void, Void> {

		@Override
		protected Void doInBackground(Object... sockets) {

			ServerSocket serverSocket = (ServerSocket) sockets[0];

			while (true){
				try {
//					Log.d("SERVER", "Connected");
					Socket socket = serverSocket.accept();
					ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
					Object[] data = (Object[]) in.readObject();

					String flag = (String) data[0];
					Log.d("SERVER", flag);

					if(flag.equals("INSERT")){
						String key = (String) data[2];
						Object[] valueToInsert = (Object[]) data[3];
//						insert(keyHash, values);
						localDB.put(key, valueToInsert);

					}

					else if(flag.equals("QUERY")){
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						String key = (String) data[1];
						Log.d("SERVER QUERY", key);
						out.writeObject(localDB.get(key));
						out.flush();
					}
					else if (flag.equals("QUERY*") || flag.equals("RECOVERY")){
						ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
						out.writeObject(new Object[]{localDB});
						out.flush();
					}

					else if(flag.equals("DELETE")){
						String keyToRemove = (String) data[1];
						localDB.remove(keyToRemove);
					}

					else if(flag.equals("REPLICATE")){
						Object[] values = (Object[]) data[2];
						localDB.put((String) values[0], new Object[]{values[1], String.valueOf(data[3]), values[2]});
						Log.d("REPLICATE FROM",  data[1] +"--"+ values[0]);
					}

//					else if(flag.equals("REPLICATE 2")){
//						Object[] values = (Object[]) data[2];
//						localDB.put((String) values[0], new Object[]{values[1], String.valueOf(2), values[2]});
//					}
					socket.close();

				} catch (Exception e){
					Log.e("SERVER", "exception caught");
					e.printStackTrace();
				}

			}
		}

	}


	private class ClientTask extends AsyncTask<Object, Void, Void> {

		@Override
		protected Void doInBackground(Object... objects) {
//            String remotePort = REMOTE_PORT0;

			try {
				String flag = (String) objects[0];
				String remotePort = (String) objects[1];


				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(remotePort));
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				Log.d("Client", objects[0] + "-" + objects[1]);
				out.writeObject(objects);
				out.flush();

				if (flag.equals("RECOVERY")) {
					String identifier = (String) objects[2];
					String replace = (String) objects[3];
					ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
					Object[] data = (Object[]) in.readObject();
					HashMap<String, Object[]> recoveryData = (HashMap<String, Object[]>) data[0];

					for (Map.Entry<String, Object[]> entry : recoveryData.entrySet()) {
						Object[] value = entry.getValue();

						if (value[1].equals(identifier)) {
//						if (localDB.containsKey(entry.getKey())) {
//							if (((Date) localDB.get(entry.getKey())[2]).compareTo((Date) value[2]) < 0) {
//								localDB.put(entry.getKey(), entry.getValue());
//								continue;
//							}
//						}
							value[1] = replace;
							localDB.put(entry.getKey(), value);
						}
					}
				}
			} catch (Exception e){
				e.printStackTrace();
			}


			return null;
		}
	}
}
