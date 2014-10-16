package client;

import java.io.*;
import java.util.*;
import org.voltdb.*;
import org.voltdb.types.*;
import org.voltdb.client.*;

public class FraudBenchmark extends BaseBenchmark {

    private Random rand = new Random();
    private long txnId = 0;
    private Long[] accounts;
    private String[] acct_states;
    private int[] amounts = {25,50,75,100,150,200,250,300};
    private PersonGenerator gen = new PersonGenerator();

    // constructor
    public FraudBenchmark(BenchmarkConfig config) {
        super(config);
    }

    // this gets run once before the benchmark begins
    public void initialize() throws Exception {

        List<Long> acctList = new ArrayList<Long>(config.custcount*2);
        List<String> stList = new ArrayList<String>(config.custcount*2);

        // generate customers
        for (int c=0; c<config.custcount; c++) {

            if (c % 10000 == 0) {
                System.out.println("  "+c);
            }

            PersonGenerator.Person p = gen.newPerson();
            //int ac = rand.nextInt(areaCodes.length);
            
            client.callProcedure(new BenchmarkCallback("CUSTOMER.insert"),
                                 "CUSTOMER.insert",
                                 c,
                                 p.firstname,
                                 p.lastname,
                                 "Anytown",
                                 p.state,
                                 p.phonenumber,
                                 p.dob,
                                 p.sex
                                 );

            int accts = rand.nextInt(5);
            for (int a=0; a<accts; a++) {
                
                int acct_no = (c*100)+a;
                client.callProcedure(new BenchmarkCallback("ACCOUNT.insert"),
                                     "ACCOUNT.insert",
                                     acct_no,
                                     c,
                                     rand.nextInt(10000),
                                     rand.nextInt(10000),
                                     new Date(),
                                     "Y"
                                     );
                acctList.add(Long.valueOf(acct_no));
                stList.add(p.state);
            }
        }
        
        accounts = acctList.toArray(new Long[acctList.size()]);
        acct_states = stList.toArray(new String[stList.size()]);

    }

    public void iterate() throws Exception {

        // pick a random account and generate a transaction
        int i = rand.nextInt(accounts.length);
        long TXN_ID = txnId++;
        long ACC_NO = accounts[i];
        double TXN_AMT = amounts[rand.nextInt(amounts.length)];
        String TXN_STATE = acct_states[i];
        String TXN_CITY = "Some City";
        TimestampType TXN_TS = new TimestampType();

        // generate "out of state" fraud
        // a small % of the time, use a random state
        if (rand.nextInt(50000) == 0) {
            TXN_STATE = gen.randomState();
        }

        client.callProcedure(new BenchmarkCallback("DetectFraud"),
                             "DetectFraud",
                             TXN_ID,ACC_NO,TXN_AMT,TXN_STATE,TXN_CITY,TXN_TS);

    }

    public static void main(String[] args) throws Exception {
        BenchmarkConfig config = BenchmarkConfig.getConfig("FraudBenchmark",args);
        
        BaseBenchmark c = new FraudBenchmark(config);
        c.runBenchmark();
    }


}
