/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class intellistream_morphstream_util_libVNFFrontend_NativeInterface */

#ifndef _Included_intellistream_morphstream_util_libVNFFrontend_NativeInterface
#define _Included_intellistream_morphstream_util_libVNFFrontend_NativeInterface
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __init_SFC
 * Signature: (I[Ljava/lang/String;)Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1init_1SFC
  (JNIEnv *, jobject, jint, jobjectArray);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __VNFThread
 * Signature: (I[Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1VNFThread
  (JNIEnv *, jobject, jint, jobjectArray);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    _execute_sa_udf
 * Signature: (JI[BI)[B
 */
JNIEXPORT jbyteArray JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1execute_1sa_1udf
  (JNIEnv *, jclass, jlong, jint, jbyteArray, jint);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __txn_finished
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1txn_1finished
  (JNIEnv *, jclass, jlong);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __txn_finished_results
 * Signature: (JI)I
 */
JNIEXPORT jint JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1txn_1finished_1results
  (JNIEnv *, jclass, jlong, jint);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __pause_txn_processing
 * Signature: (Ljava/util/HashMap;Ljava/util/HashMap;)V
 */
JNIEXPORT void JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1pause_1txn_1processing
  (JNIEnv *, jclass, jobject, jobject);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __get_state_from_cache
 * Signature: (Ljava/lang/String;)I
 */
JNIEXPORT jint JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1get_1state_1from_1cache
  (JNIEnv *, jclass, jstring);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __update_states_to_cache
 * Signature: (Ljava/lang/String;I)V
 */
JNIEXPORT void JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1update_1states_1to_1cache
  (JNIEnv *, jclass, jstring, jint);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __resume_txn_processing
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1resume_1txn_1processing
  (JNIEnv *, jclass);

/*
 * Class:     intellistream_morphstream_util_libVNFFrontend_NativeInterface
 * Method:    __request_lock
 * Signature: (ILjava/lang/String;I)I
 */
JNIEXPORT jint JNICALL Java_intellistream_morphstream_util_libVNFFrontend_NativeInterface__1_1request_1lock
  (JNIEnv *, jclass, jint, jstring, jint);

#ifdef __cplusplus
}
#endif
#endif
