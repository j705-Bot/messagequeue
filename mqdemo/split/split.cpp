#include <iostream>
#include <string>
#include <vector>

/**
 *  字符串分割函数，按照指定分隔符sep对字符串str进行分割，结果存入result
 *  str 待分割的原始字符串
 *  sep 分隔符（支持多字符分隔符）
 *  result 输出参数，保存分割完成后的子串
 *  size_t 返回分割得到的子串数量
 *
 * 算法思路：
 * 1. idx：记录本次查找的起始下标，初始为0，代表从字符串开头开始查找分隔符
 * 2. pos：使用string::find从idx位置向后查找分隔符sep，返回找到的位置下标
 * 3. 如果找不到分隔符(pos == std::string::npos)：截取idx到字符串末尾的子串，存入结果，结束分割
 * 4. 如果找到分隔符且 pos == idx：说明当前位置就是分隔符，代表空串，跳过该分隔符，更新idx继续查找
 * 5. 如果找到分隔符且 pos > idx：截取 [idx, pos) 的子串存入结果，更新idx到当前分隔符后面，继续循环
 */
size_t split(const std::string &str,const std::string &sep,std::vector<std::string> &result){
    //分割的思想:
    //1.从0位置开始查找指定字符的位置，找到之后开始分割
    //2.从上次查找的位置继续向后查找指定的字符
    size_t pos,idx = 0;
    while(idx < str.size())
    {
        // 从idx位置开始查找分隔符sep，返回分隔符所在下标
        pos = str.find(sep,idx);
        if(pos == std::string::npos)
        {
            //没有找到，则从查找位置截取到末尾，剩余全部内容作为最后一段子串
            result.push_back(str.substr(idx));
            return result.size();
        }
        //pos == idx代表两个分割符之间没有数据，或者说查找的起始位置就是分割符，跳过空串
        if(pos == idx)
        {
            idx = pos + sep.size(); // 下标移动到当前分隔符后面
            continue;
        }
        // 截取[idx, pos)区间的子串，存入结果容器
        result.push_back(str.substr(idx,pos - idx));
        idx = pos + sep.size(); // 更新查找起点到当前分隔符的后方
    }
    return result.size();
}

int main()
{
    std::string str = "...hjq...hello..##..hello";
    std::vector<std::string> arry;
    // 调用split，使用 "." 作为分隔符分割字符串，arry存放分割结果
    int n = split(str,".",arry);
    // 范围for循环遍历分割后的所有子串，打印
    for(auto &s : arry)
        std::cout << s << std::endl;
    return 0;
}
