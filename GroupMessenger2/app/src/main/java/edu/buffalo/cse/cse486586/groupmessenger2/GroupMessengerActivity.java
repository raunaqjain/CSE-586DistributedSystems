package edu.buffalo.cse.cse486586.groupmessenger2;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.provider.ContactsContract;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */

public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT0 = "11108";
    static final String REMOTE_PORT1 = "11112";
    static final String REMOTE_PORT2 = "11116";
    static final String REMOTE_PORT3 = "11120";
    static final String REMOTE_PORT4 = "11124";
//    String[] remotePorts = {REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4};
    ArrayList<String> remotePorts = new ArrayList<String>();

    public GroupMessengerActivity(){
        remotePorts.add(REMOTE_PORT0);
        remotePorts.add(REMOTE_PORT1);
        remotePorts.add(REMOTE_PORT2);
        remotePorts.add(REMOTE_PORT3);
        remotePorts.add(REMOTE_PORT4);
    }

    Integer numberOfAliveProcess = 5;
    Integer keyCounter = 0;
    String failedPort = "F";

    public class PQueue{
        String proposal;
        String message;
        String remotePort;
        boolean deliverable = false;
        boolean complete = false;

        public PQueue(String proposal, String message, String remotePort) {
            this.proposal = proposal;
            this.message = message;
            this.remotePort = remotePort;
        }

        public void setProposal(String proposal) {
            this.proposal = proposal;
        }

        public void setDeliverable(Boolean deliverable) {
            this.deliverable = deliverable;
        }

        public void setComplete(Boolean complete) {
            this.complete = complete;
        }



    }
    public class PQComparator implements Comparator<PQueue>{

        @Override
        public int compare(PQueue lhs, PQueue rhs) {
            return Integer.parseInt(lhs.proposal) - Integer.parseInt(rhs.proposal);
        }
    }

    ArrayList<PQueue> pq = new ArrayList<PQueue>();


//    ArrayList<Integer> sequenceNumber = new ArrayList<Integer>();
//    ArrayList<String> sequenceNumber = new ArrayList<String>();

    static final int SERVER_PORT = 10000;

    //Uri.Builder uriBuilder = new Uri.Builder();
    Uri.Builder builder = Uri.parse("content://edu.buffalo.cse.cse486586.groupmessenger2.provider").buildUpon();
    Uri mUri = builder.build();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());

        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
//        findViewById(R.id.button1).setOnClickListener(
//                new OnPTestClickListener(tv, getContentResolver()));
//
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.d("TELEPHONY", portStr);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.d("TELEPHONY", myPort);

        try {
            /*
             * Create a server socket as well as a thread (AsyncTask) that listens on the server
             * port.
             *
             * AsyncTask is a simplified thread construct that Android provides. Please make sure
             * you know how it works by reading
             * http://developer.android.com/reference/android/os/AsyncTask.html
             */
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            Object[] info = new Object[]{serverSocket, myPort};
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, info);
        } catch (IOException e) {
            /*
             * Log is a good way to debug your code. LogCat prints out all the messages that
             * Log class writes.
             *
             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
             * and http://developer.android.com/tools/debugging/debugging-log.html
             * for more information on debugging.
             */
            Log.e(TAG, "Can't create a ServerSocket");
            return;
        }

        /*
         * Retrieve a pointer to the input box (EditText) defined in the layout
         * XML file (res/layout/main.xml).
         *
         * This is another example of R class variables. R.id.edit_text refers to the EditText UI
         * element declared in res/layout/main.xml. The id of "edit_text" is given in that file by
         * the use of "android:id="@+id/edit_text""
         */
        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button button = (Button) findViewById(R.id.button4);

        button.setOnClickListener(new View.OnClickListener() {

            @Override
            public void onClick(View v) {

                /*
                 * If the key is pressed (i.e., KeyEvent.ACTION_DOWN) and it is an enter key
                 * (i.e., KeyEvent.KEYCODE_ENTER), then we display the string. Then we create
                 * an AsyncTask that sends the string to the remote AVD.
                 */
                String msg = editText.getText().toString() + "\n";
                editText.setText(""); // This is one way to reset the input box.


                /*
                 * Note that the following AsyncTask uses AsyncTask.SERIAL_EXECUTOR, not
                 * AsyncTask.THREAD_POOL_EXECUTOR as the above ServerTask does. To understand
                 * the difference, please take a look at
                 * http://developer.android.com/reference/android/os/AsyncTask.html
                 */
//                for (String remotePort: remotePorts) {
//                    new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, msg, remotePort);
//                }
                new ClientTask().execute(msg, myPort, String.valueOf(numberOfAliveProcess));

            }
        });

    }

    public synchronized void put(ArrayList<PQueue> pq, String proposal, String message, String remotePort){
        pq.add(new PQueue(proposal, message, remotePort));
    }

    public synchronized void update(ArrayList<PQueue> pq, String message, String finalSequenceNumber){
        for (int i = 0; i < pq.size(); i++) {
            PQueue msgs = pq.get(i);

            if (msgs.message == message) {
                msgs.setDeliverable(true);
                msgs.setProposal(finalSequenceNumber);
            }
        }
    }

    public synchronized ArrayList<PQueue> publish(ArrayList<PQueue> pq){
        Collections.sort(pq, new PQComparator());

        ArrayList<PQueue> found = new ArrayList<PQueue>();

        int len = pq.size();
        Log.d("Server length", Integer.toString(len));
        for (int i = 0; i < len; i++) {
            PQueue msgs = pq.get(i);
            if (msgs.deliverable) {
                found.add(msgs);

            } else {
                break;
            }
        }
        pq.removeAll(found);
        return found;
    }

    public synchronized void remove(ArrayList<PQueue> pq, String failedPort){
        if ("F".equals(failedPort)){
            return;
        }
        Log.d("FAILURE DETECTED", failedPort);
        ArrayList<PQueue> found = new ArrayList<PQueue>();

        for (PQueue msgs: pq){
            String prop = msgs.proposal.substring(msgs.proposal.length() - 5);
//            if (prop.equals(failedPort)){
//                found.add(msgs);
//            }
            if (msgs.remotePort.equals(failedPort)){
                found.add(msgs);
            }
        }
        pq.removeAll(found);

    }

    private class ServerTask extends AsyncTask<Object, String, Void> {
        private Integer finalCounter = 0;
        private Integer counter = 0;

        @Override
        protected Void doInBackground(Object... objects) {
            ServerSocket serverSocket = (ServerSocket) objects[0];
            final String myPort = (String) objects[1];
            Socket server;
            while (true) {
                try {
                    server = serverSocket.accept();
                    DataInputStream in = new DataInputStream(server.getInputStream());
                    String msg = in.readUTF();

                    DataOutputStream out = new DataOutputStream(server.getOutputStream());
                    out.writeUTF(String.valueOf(counter));
                    counter++;
                    out.flush();

                    in = new DataInputStream(server.getInputStream());
                    String finalProposal = in.readUTF();
                    publishProgress(msg, finalProposal);

                } catch (IOException e){
                    e.printStackTrace();
                }

            }
        }




        @Override
        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String value = strings[0].trim();
            String key = strings[1].trim();
            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(key +"--"+ value + "\t\n");

            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */
            Log.d("ONPROGRESS", keyCounter + "--" + value);
            ContentValues contentValues = new ContentValues();
            contentValues.put("key", keyCounter);
            keyCounter++;
            contentValues.put("value", value);
            getContentResolver().insert(mUri, contentValues);
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */


    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            final String myPort = msgs[1];
            final String message = msgs[0];
            ArrayList<Integer> sequenceNumber = new ArrayList<Integer>();

            ArrayList<Socket> socs = new ArrayList<Socket>();
            for(int i=0; i<remotePorts.size(); i++){
                try {
                    socs.add(new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
                            Integer.parseInt(remotePorts.get(i))));

                    DataOutputStream out = new DataOutputStream(socs.get(i).getOutputStream());
                    out.writeUTF(message);
                    out.flush();

                    DataInputStream in = new DataInputStream(socs.get(i).getInputStream());
//                            Log.d("Client isClosed?", String.valueOf(socket.isClosed()));
                    String proposal = in.readUTF();
                    Log.d("Client", "got proposal: " + proposal);

                    sequenceNumber.add(Integer.parseInt(proposal));

                } catch (IOException e) {
                    e.printStackTrace();
                } catch (Exception e){
                    Log.d("EXCEPTION", "");
                    e.printStackTrace();
                }
            }

            Integer maxProposal = Collections.max(sequenceNumber);
            for(int i=0; i<remotePorts.size(); i++) {
                DataOutputStream out = null;
                try {
                    out = new DataOutputStream(socs.get(i).getOutputStream());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    out.writeUTF(String.valueOf(maxProposal));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    out.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }


                return null;
        }
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }
}
