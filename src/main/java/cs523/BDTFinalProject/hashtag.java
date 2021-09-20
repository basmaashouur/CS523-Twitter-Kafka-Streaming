package cs523.BDTFinalProject;

import java.io.Serializable;
public class hashtag implements Serializable {
private String time;
private String hashtag;
public hashtag(String time, String hashtag) {
	super();
	this.time = time;
	this.hashtag = hashtag;
	}


public String getTime() {
	return time;
}
public void setTime(String time) {
	this.time = time;
}
public String getHashtag() {
	return hashtag;
}
public void setHashtag(String hashtag) {
	this.hashtag = hashtag;
}
}