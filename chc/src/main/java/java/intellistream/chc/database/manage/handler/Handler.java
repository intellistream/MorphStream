package java.intellistream.chc.database.manage.handler;

import java.intellistream.chc.common.dao.Packet;

/**
 * Abstract handler
 */
public abstract class Handler implements Runnable {
    public final Packet packet;

    public Handler(Packet packet) {
        this.packet = packet;
    }
}
