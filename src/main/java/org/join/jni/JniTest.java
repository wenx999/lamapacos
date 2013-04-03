/*
 * JniTest.java Create on 2011-11-28
 *
 * Copyright (c) 2011-11-28 by ZhaoYong
 *
 * @author <a href="v-aero@163.com">ZhaoYong</a>
 *
 * @version 1.0
 */

package org.join.jni;

/**
 * Java JNI简单实现
 * 
 * @author Join
 */
public class JniTest {

	// win32
	static {
		// System.loadLibrary():装载Windows\System32下或jre\bin或Tomcat\bin目录下的本地链接库
		// System.load():根据具体的目录来加截本地链接库,必须是绝对路径
		System.load(System.getProperty("user.dir") + "/lib/JniDll_win32.dll");
	}

	/** C方法并执行回调（测试用） */
	public static native void sayHelloWin(int add_x, int add_y, int sub_x,
			int sub_y);

	/** C回调Java方法（静态） */
	public static int add(int x, int y) {
		System.out.println("==Java静态add方法==");
		return x + y;
	}

	/** C回调Java方法（非静态） */
	public int sub(int x, int y) {
		System.out.println("==Java非静态sub方法==");
		return x - y;
	}

	// linux
	public static void main(String[] args) {
		sayHelloWin(3, 8, 2, 5);
	}

}
