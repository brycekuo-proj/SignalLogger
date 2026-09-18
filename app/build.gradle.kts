plugins {
    id("com.android.application")
}

android {
    namespace = "com.bryce.signallogger"
    compileSdk = 35

    defaultConfig {
        applicationId = "com.bryce.signallogger"
        minSdk = 23
        targetSdk = 35
        versionCode = 3
        versionName = "0.1.2"
    }

    buildTypes {
        release {
            isMinifyEnabled = false
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
        }
    }
}
