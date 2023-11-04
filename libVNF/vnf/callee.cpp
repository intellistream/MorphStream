/*Filename: CallbacksExample.cpp*/

#include <iostream>
#include <jni.h>
#include "com_Caller_Jcaller.h"

JNIEXPORT void JNICALL
Java_com_Caller_Jcaller_callback(JNIEnv *env, jobject jthis, jobject person)
{
	jclass thisClass = env->GetObjectClass(jthis);

	// jclass personClass = env->FindClass("com/Person/Person");
	// if ( personClass == nullptr ) {
	// 	cout << "personClass" << endl;
	// }

	// jmethodID class_constructor = env->GetMethodID(personClass, "<init>", "()V"); // no parameters
	// if ( class_constructor == nullptr ) {
	// 	cout << "class_constructor" << endl;
	// }

	// jobject personObj = env->NewObject(personClass, class_constructor);

	// auto personMethod = env->GetMethodID(personClass, "set", "(I)V");
	// env->CallVoidMethod(person, personMethod, 15);

	// jmethodID printClassVar = env->GetStaticMethodID(thisClass, "printClassVar", "(Lcom/Person/Person;)V");
	// if (NULL == printClassVar)
	// 	return;

	// env->CallVoidMethod(jthis, printClassVar, personObj);
}