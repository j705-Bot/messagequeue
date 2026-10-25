#include "../mqcommon/mq_helper.hpp"

int main()
{


    // bitmq::FileHelper helper("../mqcommon/logger.hpp");
    // ELOG("是否存在%d",helper.exists());
    // DLOG("文件大小:%ld",helper.size());
    // bitmq::FileHelper tmp_helper("./aaa/bbb/ccc/tmp.hpp");
    // if(tmp_helper.exists()== false)
    // {
    //     std::string path = bitmq::FileHelper::parentDirectory("./aaa/bbb/ccc/tmp.hpp");
    //     if(bitmq::FileHelper(path).exists() == false)
    //     {
    //         bitmq::FileHelper::createDirectory(path);
    //     }
    //     bitmq::FileHelper::createFile("./aaa/bbb/ccc/tmp.hpp");
    // }

    // std::string body;
    // helper.read(body);
    // DLOG("%s",body.c_str());
    // tmp_helper.write(body);
    bitmq::FileHelper tmp_helper("../mqcommon/logger.hpp");
    //char str[16] = {0};
    // tmp_helper.read(str,8,11);
    // DLOG("[%s]",str);
    //tmp_helper.write("12345678901",8,11);
    //tmp_helper.rename("./aaa/bbb/ccc/test.cpp");
    //bitmq::FileHelper::removeFile("./aaa/bbb/ccc/test.cpp");
    bitmq::FileHelper::removeDirectory("./aaa");
    return 0;
}