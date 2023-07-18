package common.model.ads;

import java.io.Serializable;

/**
 * @author Maycon Viana Bordin <mayconbordin@gmail.com>
 */
public class AdEvent implements Serializable {
    private static final long serialVersionUID = 2107785080405113092L;
    private Type type;
    private String displayUrl;
    private long queryId;
    private long adID;
    private long userId;
    private long advertiserId;
    private long keywordId;
    private long titleId;
    private long descriptionId;
    private int depth;
    private int position;

    public AdEvent() {
    }

    public AdEvent(String displayUrl, long queryId, long adID, long userId, long advertiserId, long keywordId, long titleId, long descriptionId, int depth, int position) {
        this.displayUrl = displayUrl;
        this.queryId = queryId;
        this.adID = adID;
        this.userId = userId;
        this.advertiserId = advertiserId;
        this.keywordId = keywordId;
        this.titleId = titleId;
        this.descriptionId = descriptionId;
        this.depth = depth;
        this.position = position;
    }

    public Type getType() {
        return type;
    }

    public void setType(Type type) {
        this.type = type;
    }

    public String getDisplayUrl() {
        return displayUrl;
    }

    public void setDisplayUrl(String displayUrl) {
        this.displayUrl = displayUrl;
    }

    public long getQueryId() {
        return queryId;
    }

    public void setQueryId(long queryId) {
        this.queryId = queryId;
    }

    public long getAdID() {
        return adID;
    }

    public void setAdID(long adID) {
        this.adID = adID;
    }

    public long getUserId() {
        return userId;
    }

    public void setUserId(long userId) {
        this.userId = userId;
    }

    public long getAdvertiserId() {
        return advertiserId;
    }

    public void setAdvertiserId(long advertiserId) {
        this.advertiserId = advertiserId;
    }

    public long getKeywordId() {
        return keywordId;
    }

    public void setKeywordId(long keywordId) {
        this.keywordId = keywordId;
    }

    public long getTitleId() {
        return titleId;
    }

    public void setTitleId(long titleId) {
        this.titleId = titleId;
    }

    public long getDescriptionId() {
        return descriptionId;
    }

    public void setDescriptionId(long descriptionId) {
        this.descriptionId = descriptionId;
    }

    public int getDepth() {
        return depth;
    }

    public void setDepth(int depth) {
        this.depth = depth;
    }

    public int getPosition() {
        return position;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return "AdEvent{" + "type=" + type + ", displayUrl=" + displayUrl + ", queryId=" + queryId + ", adID=" + adID + ", userId=" + userId + ", advertiserId=" + advertiserId + ", keywordId=" + keywordId + ", titleId=" + titleId + ", descriptionId=" + descriptionId + ", depth=" + depth + ", position=" + position + '}';
    }

    public enum Type {Click, Impression}
}
