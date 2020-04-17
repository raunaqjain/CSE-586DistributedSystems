package edu.buffalo.cse.cse486586.simpledht;

import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigInteger;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Formatter;
import java.util.Iterator;
import java.util.TreeSet;

import android.annotation.SuppressLint;
import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDhtProvider extends ContentProvider {

    static final int SERVER_PORT = 10000;
    private static final String KEY_FIELD = "key";
    private static final String VALUE_FIELD = "value";

    static final String TAG = SimpleDhtActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
    ArrayList<String> remotePorts = new ArrayList<String>(5);

    public SimpleDhtProvider(){
        remotePorts.add(REMOTE_PORT0);
        remotePorts.add(REMOTE_PORT1);
        remotePorts.add(REMOTE_PORT2);
        remotePorts.add(REMOTE_PORT3);
        remotePorts.add(REMOTE_PORT4);
    }

//    public class TheComparator implements Comparator<BigInteger>, Serializable {
//
//        @Override
//        public int compare(BigInteger lhs, BigInteger rhs) {
//            return lhs.compareTo(rhs);
//        }
//    }
//
//    TreeSet<BigInteger> aliveNodes = new TreeSet<BigInteger>(new TheComparator());
    TreeSet<BigInteger> aliveNodes = new TreeSet<BigInteger>();

    BigInteger myHash;
    BigInteger succ;
    BigInteger prev;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub
        String key = (String) values.get(KEY_FIELD);
        String value = (String) values.get(VALUE_FIELD);

        try {
            BigInteger keyHash = new BigInteger(genHash(key), 16);
            if (keyHash.compareTo(myHash) < 0){
                String file = getContext().getCacheDir().toString() + "/" + values.get("key").toString() + ".txt";
                FileWriter fileWriter = new FileWriter(file);
                fileWriter.write(values.get("value").toString());
                fileWriter.flush();
                fileWriter.close();
            }
            else {
                new ClientTask().execute("INSERT", succ);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public void nodeJoin(String portNumber) {
        try {
            BigInteger nodeHash = new BigInteger(genHash(portNumber), 16);
            Log.d("HASH", portNumber + "-" + nodeHash);
            myHash = nodeHash;
            aliveNodes.add(nodeHash);
            new ClientTask().execute("JOIN", portNumber, nodeHash);


        } catch (NoSuchAlgorithmException e) {
            Log.e("NodeJoin error", "NoSuchAlgorithmException");
        }
    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        final String currPort = String.valueOf((Integer.parseInt(portStr)*2));
        Log.d("MAIN", portStr);

        try{

            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Object[] serverPort = new Object[] {serverSocket, currPort};
            Log.d("Connecting to Server", "");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverPort);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        nodeJoin(portStr);


        return false;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        return null;
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

    public void bMulticastMessage(Object[] msg, ArrayList<String> pt){

        Log.d("In BMulticast", "");
        for(int i = 0; i <=pt.size()-1; i++) {
            try {
                // send happens here
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                        Integer.parseInt(pt.get(i)));
                Log.d("BMulticast", pt.get(i));
//                Object msgToSend = msg;
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(msg);
                out.flush();
                socket.close();

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }
        }
    }

    private class ServerTask extends AsyncTask<Object, Void, Void> {

        @Override
        protected Void doInBackground(Object... sockets) {

            ServerSocket serverSocket = (ServerSocket) sockets[0];

            while (true){
                try {
                    Log.d("SERVER", "Connected");
                    Socket socket = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    Object[] data = (Object[]) in.readObject();

                    String flag = (String) data[0];
                    Log.d("SERVER", flag);

                    if (flag.equals("JOIN")){
                        aliveNodes.add((BigInteger) data[2]);
                        Log.d("SERVER", data[0] + "-" + data[1]);
                        System.out.println(aliveNodes);
                        updateNeighbours();
                        System.out.println(aliveNodes);
                        Log.d("SUCC", String.valueOf(succ));
                        Log.d("PREV", String.valueOf(prev));
                        bMulticastMessage(new Object[] {"JOIN MULTICAST", aliveNodes}, remotePorts);
                        Log.d("Bmulticast", "called");
                    }

                    else if(flag.equals("JOIN MULTICAST")){
                        aliveNodes = (TreeSet<BigInteger>) data[1];
                        System.out.println(aliveNodes);
                        updateNeighbours();
                        Log.d("Server", "JOIN MULTICAST");
                        System.out.println(aliveNodes);
                        Log.d("SUCC", String.valueOf(succ));
                        Log.d("PREV", String.valueOf(prev));

                    }

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
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT0));

                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());

                Log.d("Client", objects[0] + "-" + objects[1]);
                out.writeObject(objects);
                out.flush();


            } catch (Exception e){
                e.printStackTrace();
            }


            return null;
        }
    }
}

