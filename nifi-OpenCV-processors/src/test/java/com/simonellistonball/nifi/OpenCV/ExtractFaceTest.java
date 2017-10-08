package com.simonellistonball.nifi.OpenCV;

import java.io.InputStream;
import java.util.List;

import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class ExtractFaceTest {

    private static final String face_resource_path = "/face.png";
    private static final String no_face_resource_path = "/no_face.jpg";

    private static final String face_cascade_name = "/haarcascade_frontalface_alt.xml";

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ExtractFaces.class);
    }

    @Test
    public void testHasFace() {
        runTest(face_resource_path);
        testRunner.assertAllFlowFilesTransferred(ExtractFaces.REL_MATCH);
        
        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(ExtractFaces.REL_MATCH);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertAttributeEquals("faces.count", "1");
        }
    }
    
    @Test
    public void testMarksFace() {
    		testRunner.setProperty(ExtractFaces.MARK_FACES, "true");
        runTest(face_resource_path);
        testRunner.assertAllFlowFilesTransferred(ExtractFaces.REL_MATCH);
        List<MockFlowFile> flowFilesForRelationship = testRunner.getFlowFilesForRelationship(ExtractFaces.REL_MATCH);
        for (MockFlowFile flowFile : flowFilesForRelationship) {
            flowFile.assertAttributeEquals("faces.count", "1");
        }
    }

    @Test
    public void testNoFace() {
        runTest(no_face_resource_path);
        testRunner.assertAllFlowFilesTransferred(ExtractFaces.REL_UNMATCH);
        testRunner.assertTransferCount(ExtractFaces.REL_MATCH, 0);
    }

    private void runTest(String image) {
        String face_cascade_path = ExtractFaceTest.class.getResource(face_cascade_name).getPath();
        testRunner.setProperty(ExtractFaces.CASCADER, face_cascade_path);
        InputStream resourceAsStream = ExtractFaces.class.getResourceAsStream(image);
        testRunner.enqueue(resourceAsStream);
        testRunner.run();
    }

    	// TODO - write a test for the faces relation
    
}
