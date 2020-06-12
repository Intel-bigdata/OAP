/*
 * Filename:
 * /mnt/spark-pmof/Spark-PMoF/rpmp/pmpool/client/native/com_intel_rpmp_PmPoolClient.h
 * Path: /mnt/spark-pmof/Spark-PMoF/rpmp/pmpool/client/native
 * Created Date: Thursday, March 5th 2020, 10:44:12 am
 * Author: root
 *
 * Copyright (c) 2020 Intel
 */

#include <jni.h>
/* Header for class com_intel_rpmp_PmPoolClient */

#ifndef PMPOOL_CLIENT_NATIVE_COM_INTEL_RPMP_PMPOOLCLIENT_H_
#define PMPOOL_CLIENT_NATIVE_COM_INTEL_RPMP_PMPOOLCLIENT_H_
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    newPmPoolClient_
 * Signature: (Ljava/lang/String;Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_newPmPoolClient_1(
    JNIEnv *, jobject, jstring, jstring);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    alloc_
 * Signature: (JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_alloc_1(JNIEnv *,
                                                                 jobject, jlong,
                                                                 jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    free_
 * Signature: (JJ)I
 */
JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_free_1(JNIEnv *,
                                                               jobject, jlong,
                                                               jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    write_
 * Signature: (JLjava/lang/String;JJ)I
 */
JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_write_1(JNIEnv *,
                                                                jobject, jlong,
                                                                jstring, jlong,
                                                                jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    alloc_and_write_
 * Signature: (Ljava/lang/String;JJ)J
 */
JNIEXPORT jlong JNICALL
Java_com_intel_rpmp_PmPoolClient_alloc_1and_1write_1__Ljava_lang_String_2JJ(
    JNIEnv *, jobject, jstring, jlong, jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    alloc_and_write_
 * Signature: (Ljava/nio/ByteBuffer;JJ)J
 */
JNIEXPORT jlong JNICALL
Java_com_intel_rpmp_PmPoolClient_alloc_1and_1write_1__Ljava_nio_ByteBuffer_2JJ(
    JNIEnv *, jobject, jobject, jlong, jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    put
 * Signature: (Ljava/lang/String;Ljava/nio/ByteBuffer;JJ)J
 */
JNIEXPORT jlong JNICALL Java_com_intel_rpmp_PmPoolClient_put(JNIEnv *, jobject,
                                                             jstring, jobject,
                                                             jlong, jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    getMeta
 * Signature: (Ljava/lang/String;J)[J
 */
JNIEXPORT jlongArray JNICALL Java_com_intel_rpmp_PmPoolClient_getMeta(JNIEnv *,
                                                                      jobject,
                                                                      jstring,
                                                                      jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    del
 * Signature: (Ljava/lang/String;J)I
 */
JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_del(JNIEnv *, jobject,
                                                            jstring, jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    read_
 * Signature: (JJLjava/nio/ByteBuffer;J)I
 */
JNIEXPORT jint JNICALL Java_com_intel_rpmp_PmPoolClient_read_1(JNIEnv *,
                                                               jobject, jlong,
                                                               jlong, jobject,
                                                               jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    shutdown_
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_shutdown_1(JNIEnv *,
                                                                   jobject,
                                                                   jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    waitToStop_
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_waitToStop_1(JNIEnv *,
                                                                     jobject,
                                                                     jlong);

/*
 * Class:     com_intel_rpmp_PmPoolClient
 * Method:    dispose_
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_com_intel_rpmp_PmPoolClient_dispose_1(JNIEnv *,
                                                                  jobject,
                                                                  jlong);

#ifdef __cplusplus
}
#endif
#endif  // PMPOOL_CLIENT_NATIVE_COM_INTEL_RPMP_PMPOOLCLIENT_H_
