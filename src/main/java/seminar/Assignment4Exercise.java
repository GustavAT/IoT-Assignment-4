package seminar;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.json.simple.parser.ParseException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;


public class Assignment4Exercise {
	
	static KeyPair keyPair;
	static AmazonEC2 ec2;

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ParseException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, ParseException, InterruptedException {
	
		
	/*****************Load the credentials****************/
		
	AWSCredentials credentials = null;
        try 
	{
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } 
	catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (C:/Users/ASUS/.aws/credentials), and is in valid format.",
                    e);
        }
        

	/*****************List the availability zones****************/
        
	ec2 = new AmazonEC2Client(credentials);
	DescribeAvailabilityZonesResult availabilityZonesResult = ec2.describeAvailabilityZones();
	//List zones = availabilityZonesResult.getAvailabilityZones();

	/* As part of this assignment you should extend this section of the code to support
	 * showing the available zones */
        

	/*****************Set an AWS region****************/
	/* As part of this assignment you should extend this section of the code to support
	 * setting the AWS zone through the SDK */
        
	
	/*****************Set a filter on available AMIs/VMIs****************/
	/* As part of this assignment you should extend this section of the code to support
	 * setting filters of the available AMIs. Below you can find simple example on creating filter Object
	Filter Filter1 = new Filter().withName("platform").withValues("windows");*/
			
		
	/*****************Create new security group****************/
	String gid = null;

		try 
		{
	    	CreateSecurityGroupRequest csgr = new CreateSecurityGroupRequest();
	    	csgr.withGroupName("JavaSecurityGroup"+(Math.random())).withDescription("Assignment4 security group");
	    	CreateSecurityGroupResult resultsc = ec2
	    			.createSecurityGroup(csgr);
	    	System.out.println(String.format("Security group created: [%s]",
	    			resultsc.getGroupId()));
	        gid = resultsc.getGroupId();
        } 
		catch (AmazonServiceException ase) 
		{
			System.out.println(ase.getMessage());
        }
	

		List<String> groupNames = new ArrayList<String>();
		groupNames.add(gid);
		/* Please extend this section to check if you have already created a group*/ 

     
      
		/*****************Set incoming traffic policy****************/
		IpPermission ipPermission = new IpPermission();

		IpRange ipRange1 = new IpRange().withCidrIp("0.0.0.0/0"); // Set your IP here for security

		ipPermission.withIpv4Ranges(Arrays.asList(new IpRange[] {ipRange1}))
    		            .withIpProtocol("tcp")
    		            .withFromPort(22)
    		            .withToPort(22);
		/* Please extend this section to condifg the security group of a required web server*/
      
    	/*****************Authorize ingress traffic****************/

    	try 
    	{
        	AuthorizeSecurityGroupIngressRequest ingressRequest = new AuthorizeSecurityGroupIngressRequest();
        	ingressRequest.withGroupId(gid).withIpPermissions(ipPermission);
        	ec2.authorizeSecurityGroupIngress(ingressRequest);
        	System.out.println(String.format("Ingress port authroized: [%s]",
        			ipPermission.toString()));
        }
    	catch (AmazonServiceException ase) 
    	{
    		System.out.println(ase.getMessage());
    	}
        
        /*****************create a key for the VM ****************/
        CreateKeyPairRequest newKReq = new CreateKeyPairRequest();
        newKReq.setKeyName("Assignment4"+(Math.random()));
        CreateKeyPairResult kresult = ec2.createKeyPair(newKReq);
        keyPair=kresult.getKeyPair();
        System.out.println("Key for the VM was created  = "
        		+ keyPair.getKeyName() + "\nthe fingerprint is="
        		+ keyPair.getKeyFingerprint() + "\nthe material is= \n"
        		+ keyPair.getKeyMaterial());
       
        /*****************store the key in a .pem file ****************/
        String keyname = keyPair.getKeyName();
        String fileName="C:\\Users\\ASUS\\.aws"+keyname+".pem"; 
        File distFile = new File(fileName); 
        BufferedReader bufferedReader = new BufferedReader(new StringReader(keyPair.getKeyMaterial()));
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(distFile)); 
        char buf[] = new char[1024];        
        int len; 
        while ((len = bufferedReader.read(buf)) != -1) 
        {
        	bufferedWriter.write(buf, 0, len); 
        } 
        bufferedWriter.flush(); 
        bufferedReader.close(); 
        bufferedWriter.close(); 
        
        /*****************Start a given free tier instance****************/
        String AMIid = "ami-0e01ce4ee18447327";
        int mIC=1;
        int maxIC=1;
        RunInstancesRequest rir = new RunInstancesRequest (AMIid,mIC,maxIC);
        rir.setInstanceType("t2.micro");
        rir.setKeyName(newKReq.getKeyName());
        rir.withSecurityGroupIds(groupNames);
        
        RunInstancesResult result = ec2.runInstances(rir);
        System.out.println("Sleep");
        Thread.currentThread().sleep(5000);
  
        List <Instance> resin= result.getReservation().getInstances();
        String createdInstanceId = null;
        for (Instance ins : resin)
		{
			createdInstanceId = ins.getInstanceId();
			System.out.println("New instance has been created: "+ins.getInstanceId());
        }
        
        
        /*****************Create EBS volume for the instance****************/
        /* As part of this assignment you should extend this section of the code to support
		 * setting EBS volume for the instance.
		 */
        
        /*****************Print the public IP and DNS of the instance****************/
        /* As part of this assignment you should extend this section of the code to support
		 * printing of the publicIP and DNS of the instance.
		 */
        
        /*****************Terminate the instance after given time period****************/
        /* As part of this assignment you should extend this section of the code to support
		 * terminating the instance after a given time period.
		 */	
	}
}