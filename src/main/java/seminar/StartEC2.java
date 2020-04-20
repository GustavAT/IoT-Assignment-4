package seminar;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnull;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.AttachVolumeRequest;
import com.amazonaws.services.ec2.model.AttachVolumeResult;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.AvailabilityZone;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.CreateKeyPairResult;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupResult;
import com.amazonaws.services.ec2.model.CreateVolumeRequest;
import com.amazonaws.services.ec2.model.CreateVolumeResult;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeImagesRequest;
import com.amazonaws.services.ec2.model.DescribeImagesResult;
import com.amazonaws.services.ec2.model.DescribeKeyPairsRequest;
import com.amazonaws.services.ec2.model.DescribeKeyPairsResult;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Image;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.IpRange;
import com.amazonaws.services.ec2.model.KeyPair;
import com.amazonaws.services.ec2.model.KeyPairInfo;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.ec2.model.VolumeType;

public class StartEC2 {

	private static final Logger logger = Logger.getLogger(StartEC2.class.getName());

	private static final String IMAGE_ID_UBUNTU = "ami-07ebfd5b3428b6f4d";
	private static final String SECURITY_GROUP_NAME = "assignment4";
	private static final String KEY_PAIR_NAME = "assignment4";

	public static void main(String[] args) {

		final AmazonEC2 ec2Client = createStandardEC2Client();

		logger.log(Level.INFO, "EC2 client initialized");

		listAllAvailabilityZones(ec2Client);

		listAMIsFiltered(ec2Client);

		final String securityGroupId = ensureSecurityGroup(ec2Client);
		final List<IpPermission> permissions = Arrays.asList(
				createIpPermission(22),
				createIpPermission(80)
		);
		authorizeSecurityGroupIngressRequest(ec2Client, securityGroupId, permissions);

		ensureKeyPair(ec2Client);

		final Instance instance = launchInstance(ec2Client, securityGroupId);

		waitSeconds(20);

		printInstanceInfo(instance);
		attachEbsVolume(ec2Client, instance);

		waitSeconds(20);

		stopInstance(ec2Client, instance);

	}

	private static void waitSeconds(final int seconds) {
		logger.log(Level.INFO, "Wait for {0} seconds", seconds);

		try {
			Thread.sleep(seconds * 1000);
		} catch (final Exception e) {
			// ignore
		}
	}

	/**
	 * Creates a default {@code AmazonEC2} client with {@code Regions.US_WEST_1} and
	 * the standard credentials loaded from ~/.aws/credentials.
	 * @return Amazon EC2 client
	 */
	@Nonnull
	private static AmazonEC2 createStandardEC2Client() {
		return AmazonEC2ClientBuilder.standard()
				.withRegion(Regions.US_EAST_1)
				.build();
	}

	/**
	 * List all available zones for given EC2 {@code client}
	 * @param client Amazon EC2 client
	 */
	private static void listAllAvailabilityZones(@Nonnull final AmazonEC2 client) {
		final DescribeAvailabilityZonesResult result = client.describeAvailabilityZones();

		for (final AvailabilityZone zone : result.getAvailabilityZones()) {
			final String message = "Zone " +
					zone.getZoneName() +
					" with status " +
					zone.getState() +
					" in region " +
					zone.getRegionName();

			logger.log(Level.INFO, message);
		}
	}

	/**
	 * List all AMIs filterd by the windows platform.
	 * @param client Amazon EC2 client
	 */
	private static void listAMIsFiltered(@Nonnull final AmazonEC2 client) {
		// Ubuntu Server 18.04 LTS (HVM), SSD Volume Type x64
		final Filter amiFilter = new Filter("image-id", Collections.singletonList(IMAGE_ID_UBUNTU));
		final DescribeImagesRequest request = new DescribeImagesRequest()
				.withFilters(amiFilter);

		final DescribeImagesResult result = client.describeImages(request);

		for (final Image image : result.getImages()) {
			final String message = "Image " +
					image.getName() +
					" with id " +
					image.getImageId() +
					" on platform " +
					image.getPlatform();

			logger.log(Level.INFO, message);
		}
	}

	/**
	 * Get or create a security group with name {@code SECURITY_GROUP}
	 * @param client Amazon EC2 client
	 * @return Id of security group
	 */
	@Nonnull
	private static String ensureSecurityGroup(@Nonnull final AmazonEC2 client) {
		final DescribeSecurityGroupsResult result = client.describeSecurityGroups();

		final Optional<SecurityGroup> group = result.getSecurityGroups()
				.stream()
				.filter(g -> SECURITY_GROUP_NAME.equals(g.getGroupName()))
				.findFirst();

		if (group.isPresent()) {
			final String groupId = group.get().getGroupId();
			logger.log(Level.INFO, "Security group found with id {0}", groupId);

			return groupId;
		}

		final CreateSecurityGroupRequest request = new CreateSecurityGroupRequest()
				.withGroupName(SECURITY_GROUP_NAME)
				.withDescription("Assignment 4 security group");
		final CreateSecurityGroupResult createResult = client.createSecurityGroup(request);

		logger.log(Level.INFO, "Created new security group with id {0}", createResult.getGroupId());

		return createResult.getGroupId();
	}

	/**
	 * Create an ip permission that allows access to all ips with tcp protocol and port 22
	 * @return IP permission
	 */
	private static IpPermission createIpPermission(final int port) {
		final IpRange ipRange = new IpRange().withCidrIp("0.0.0.0/0");

		return new IpPermission()
				.withIpv4Ranges(ipRange)
				.withIpProtocol("tcp")
				.withFromPort(port)
				.withToPort(port);
	}

	private static void authorizeSecurityGroupIngressRequest(
			@Nonnull final AmazonEC2 client,
			@Nonnull final String securityGroupId,
			@Nonnull final List<IpPermission> ipPermissions
	) {
		final AuthorizeSecurityGroupIngressRequest request = new AuthorizeSecurityGroupIngressRequest()
				.withGroupId(securityGroupId)
				.withIpPermissions(ipPermissions);

		try {
			client.authorizeSecurityGroupIngress(request);

			logger.log(Level.INFO, "Security rule created");
		} catch (final Exception e) {
			logger.log(Level.INFO, "Security rule already exists", e.fillInStackTrace());
		}
	}

	/**
	 * Get or create a new key-pair
	 * @param client Amazon EC2 client
	 */
	private static void ensureKeyPair(@Nonnull final AmazonEC2 client) {
		final DescribeKeyPairsRequest request = new DescribeKeyPairsRequest()
				.withKeyNames(KEY_PAIR_NAME);

		try {
			final DescribeKeyPairsResult result = client.describeKeyPairs(request);
			final KeyPairInfo keyPairInfo = result.getKeyPairs().iterator().next();

			logger.log(Level.INFO, "Existing key-pair found {0}", keyPairInfo);

			return;
		} catch (final Exception e) {
			// ignore
			logger.log(Level.INFO, "Could not find existing key pair with name " + KEY_PAIR_NAME, e.fillInStackTrace());
		}

		final CreateKeyPairRequest createRequest = new CreateKeyPairRequest()
				.withKeyName(KEY_PAIR_NAME);
		final CreateKeyPairResult createResult = client.createKeyPair(createRequest);
		final KeyPair keyPair =  createResult.getKeyPair();
		logger.log(Level.INFO, "Created a new key-pair {0}", keyPair);

		writeKeyPairToFileSystem(keyPair);
	}


	/**
	 * Writes given {@code keyPair} to user's home directory Documents folder.
	 * @param keyPair Key-pair
	 */
	private static void writeKeyPairToFileSystem(@Nonnull final KeyPair keyPair) {
		final String path = System.getProperty("user.home") + "/Documents";
		final File file = new File(path);

		final Reader reader = new BufferedReader(new StringReader(keyPair.getKeyMaterial()));
		try (final Writer writer = new BufferedWriter(new FileWriter(file))) {
			final char[] buffer = new char[1024];
			int length;

			while ((length = reader.read(buffer)) != -1) {
				writer.write(buffer,0 , length);
			}

			writer.flush();
			reader.close();
			reader.close();

			logger.log(Level.INFO, "Written key-pair material to file {0}", path);
		} catch (final Exception e) {
			logger.log(Level.INFO, "Could not write key-pair material to file {0}", path);
		}
	}

	/**
	 * Launch an Ubuntu 18.04 x64 t2 nano instance
	 * @param client Amazon EC2 client
	 * @param securityGroupId Security group id
	 * @return Launched instance
	 */
	private static Instance launchInstance(@Nonnull final AmazonEC2 client, @Nonnull final String securityGroupId) {
		final RunInstancesRequest request = new RunInstancesRequest()
				.withImageId(IMAGE_ID_UBUNTU)
				.withMinCount(1)
				.withMaxCount(1)
				.withInstanceType(InstanceType.T2Nano)
				.withKeyName(KEY_PAIR_NAME)
				.withSecurityGroupIds(securityGroupId);

		final RunInstancesResult result = client.runInstances(request);

		final Instance instance = result.getReservation().getInstances().iterator().next();

		logger.log(Level.INFO, "Started instance {0}", instance);

		return instance;
	}

	/**
	 * Print info such as public IP and DNS of given {@code instance}.
	 * @param instance Running instance
	 */
	private static void printInstanceInfo(@Nonnull final Instance instance) {
		final String message = "Instance: " +
				instance.getInstanceId() +
				" with public IP " +
				instance.getPublicIpAddress() +
				" and public DNS " +
				instance.getPublicDnsName();

		logger.log(Level.INFO, "Instance info: {0}", message);
	}

	/**
	 * Stop given {@code instance}.
	 * @param client Amazon EC2 client
	 * @param instance Running instance
	 */
	private static void stopInstance(@Nonnull final AmazonEC2 client, @Nonnull final Instance instance) {
		final TerminateInstancesRequest request = new TerminateInstancesRequest()
				.withInstanceIds(instance.getInstanceId());

		client.terminateInstances(request);

		logger.log(Level.INFO, "Terminated instance {0}", instance);
	}

	/**
	 * Attach a new EBS volume standard with 8GiB to given {@code instance}.
	 * @param client Amazon EC2 client
	 * @param instance Running instance
	 */
	private static void attachEbsVolume(@Nonnull final AmazonEC2 client, @Nonnull final Instance instance) {
		final CreateVolumeRequest createVolumeRequest = new CreateVolumeRequest()
				.withVolumeType(VolumeType.Standard)
				.withAvailabilityZone("us-east-1c")
				.withSize(8);

		final CreateVolumeResult createVolumeResult = client.createVolume(createVolumeRequest);

		logger.log(Level.INFO, "Volume created {0}", createVolumeRequest);

		waitSeconds(20);

		final AttachVolumeRequest attachVolumeRequest = new AttachVolumeRequest()
				.withInstanceId(instance.getInstanceId())
				.withVolumeId(createVolumeResult.getVolume().getVolumeId())
				.withDevice("/dev/sdi");

		final AttachVolumeResult result = client.attachVolume(attachVolumeRequest);

		logger.log(Level.INFO, "Attached volume {0} to running instance", result.getAttachment());
	}
}
