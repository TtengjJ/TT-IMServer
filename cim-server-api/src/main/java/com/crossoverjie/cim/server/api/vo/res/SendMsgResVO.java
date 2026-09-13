package com.crossoverjie.cim.server.api.vo.res;

/**
 * Function:
 *
 * @author crossoverJie
 *         Date: 2017/6/26 15:43
 * @since JDK 1.8
 */
public class SendMsgResVO {
    private String msg;

    /** 本机 Channel 存在并已发起写出时为 true；无连接为 false，Route 据此不 markSent */
    private boolean pushed;

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public boolean isPushed() {
        return pushed;
    }

    public void setPushed(boolean pushed) {
        this.pushed = pushed;
    }
}
