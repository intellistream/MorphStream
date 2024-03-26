#include <jni.h>
#include <stdio.h>
#include <client.h>
#include <intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary.h>

/*
 * Class:     intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary
 * Method:    init
 * Signature: ()J
 */
JNIEXPORT jlong JNICALL Java_intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary_init (JNIEnv *, jobject)
{
printf("init\n");
}

/*
 * Class:     intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary
 * Method:    connect
 * Signature: ()Z
 */
JNIEXPORT jboolean JNICALL Java_intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary_connect (JNIEnv *, jobject)
{
printf("connect\n");
}

/*
 * Class:     intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary
 * Method:    write
 * Signature: (Ljava/lang/String;Ljava/lang/String;I)V
 */
JNIEXPORT void JNICALL Java_intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary_write (JNIEnv *, jobject, jstring, jstring, jint)
{
printf("write\n");
}

/*
 * Class:     intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary
 * Method:    read
 * Signature: (Ljava/lang/String;Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_intellistream_morphstream_engine_db_impl_remote_RemoteCallLibrary_read (JNIEnv *, jobject, jstring, jstring)
{
printf("read\n");
}