package com.simonellistonball.nifi.OpenCV;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import nu.pattern.OpenCV;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.ObjectHolder;
import org.opencv.core.Core;
import org.opencv.core.Mat;
import org.opencv.core.MatOfByte;
import org.opencv.core.MatOfRect;
import org.opencv.core.Rect;
import org.opencv.core.Size;
import org.opencv.highgui.Highgui;
import org.opencv.imgproc.Imgproc;
import org.opencv.objdetect.CascadeClassifier;

@Tags({ "image" })
@CapabilityDescription("Find faces in the image")
@WritesAttributes({ @WritesAttribute(attribute = "faces.count", description = "Number of faces in the image") })
public class ExtractFaces extends AbstractProcessor {

	public static final Relationship REL_MATCH = new Relationship.Builder()
			.name("matched")
			.description(
					"Faces have been found. FlowFiles have the face locations added to attributes")
			.build();
	public static final Relationship REL_UNMATCH = new Relationship.Builder()
			.name("unmatched")
			.description(
					"Faces have been found. FlowFiles have the face locations added to attributes")
			.build();

	public static final PropertyDescriptor CASCADER = new PropertyDescriptor.Builder()
			.name("Cascader")
			.description("File path to a haarcascader model for face detection")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.addValidator(StandardValidators.FILE_EXISTS_VALIDATOR).build();

	private static final PropertyDescriptor MARK_FACES = new PropertyDescriptor.Builder()
			.name("Mark Faces")
			.description(
					"If set true, draws a rectangle around detected faces and passes this on")
			.allowableValues("true", "false").defaultValue("false")
			.required(true).build();

	private List<PropertyDescriptor> descriptors;
	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(CASCADER);
		descriptors.add(MARK_FACES);

		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_MATCH);
		relationships.add(REL_UNMATCH);
		this.relationships = Collections.unmodifiableSet(relationships);

		OpenCV.loadShared();
		System.loadLibrary(Core.NATIVE_LIBRARY_NAME);

	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@Override
	public void onTrigger(ProcessContext context, ProcessSession session)
			throws ProcessException {
		final CascadeClassifier face_cascade = new CascadeClassifier();
		String face_cascade_name = context.getProperty(CASCADER).getValue();
		face_cascade.load(face_cascade_name);

		FlowFile flowFile = session.get();

		final ObjectHolder<Throwable> error = new ObjectHolder<>(null);
		final ObjectHolder<List<Rect>> value = new ObjectHolder<>(null);

		session.read(flowFile, new InputStreamCallback() {
			@Override
			public void process(final InputStream rawIn) throws IOException {
				try (final InputStream in = new BufferedInputStream(rawIn)) {
					byte[] temporaryImageInMemory = IOUtils.toByteArray(in);
					Mat outputImage = Highgui.imdecode(new MatOfByte(
							temporaryImageInMemory), Highgui.IMREAD_GRAYSCALE);
					MatOfRect faces = new MatOfRect();
					Imgproc.equalizeHist(outputImage, outputImage);
					face_cascade.detectMultiScale(outputImage, faces, 1.1, 2,
							0, new Size(30, 30), new Size());
					value.set(faces.toList());
				} catch (final Exception e) {
					error.set(e);
				}
			}

		});

		List<Rect> list = value.get();
		if (list.size() == 0) {
			session.transfer(flowFile, REL_UNMATCH);
		} else {
			// TODO if the split faces relation is connected, send each face
			// matrix to this as a separate FlowFile

			flowFile = session.putAttribute(flowFile, "faces.count",
					String.valueOf(list.size()));
			session.transfer(flowFile, REL_MATCH);
		}
	}
}
