#ifndef __M_ROUTE_H__
#define __M_ROUTE_H__
#include <iostream>
#include <string>
#include <vector>
#include "../mqcommon/mq_logger.hpp"
#include "../mqcommon/mq_helper.hpp"
#include "../mqcommon/mq_msg.pb.h"

namespace bitmq
{
    class Router
    {
    public:
        // ---------------------------------------------------------------
        //  1. routing_key 的合法性：只允许 a~z A~Z 0~9 . _
        // ---------------------------------------------------------------
        static bool isLegalRoutingKey(const std::string &routing_key)
        {
            for (auto &ch : routing_key)
            {
                if ((ch >= 'a' && ch <= 'z') ||
                    (ch >= 'A' && ch <= 'Z') ||
                    (ch >= '0' && ch <= '9') ||
                    (ch == '.' || ch == '_'))
                {
                    continue;
                }
                return false;
            }
            return true;
        }

        // ---------------------------------------------------------------
        //  2. binding_key 的合法性：比上面多允许 * 和 #
        // ---------------------------------------------------------------
        static bool isLegalBindingKey(const std::string &binding_key)
        {
            for (auto &ch : binding_key)
            {
                if ((ch >= 'a' && ch <= 'z') ||
                    (ch >= 'A' && ch <= 'Z') ||
                    (ch >= '0' && ch <= '9') ||
                    (ch == '.' || ch == '_') ||
                    (ch == '*' || ch == '#'))
                {
                    continue;
                }
                return false;
            }

            std::vector<std::string> sub_words;
            StrHelper::split(binding_key, ".", sub_words);

            for (size_t i = 0; i < sub_words.size(); i++)
            {
                const std::string &word = sub_words[i];

                // (1) * 和 # 必须独立成段："news.music#.*" 这种非法
                if (word.size() > 1 &&
                    (word.find('*') != std::string::npos ||
                     word.find('#') != std::string::npos))
                {
                    return false;
                }

                // (2) # 不能和任何通配符相邻；* 和 * 相邻是合法的
                //      原来从 i=0 开始访问 sub_words[i-1] → 越界；必须从 i=1 开始
                //      原来检查了 "**"（但 * 和 * 相邻其实合法），却漏了 "#*"
                if (i > 0)
                {
                    if ((word == "#" && sub_words[i - 1] == "*") ||   // #*
                        (word == "#" && sub_words[i - 1] == "#") ||   // ##
                        (word == "*" && sub_words[i - 1] == "#"))     // *#
                    {
                        return false;
                    }
                }
            }
            return true;
        }

        // ---------------------------------------------------------------
        //  3. 路由匹配
        //  
        // ---------------------------------------------------------------
        static bool route(ExchangeType type, const std::string &routing_key,
                          const std::string &binding_key)
        {
            if (type == ExchangeType::DIRECT)
            {
                return routing_key == binding_key;
            }
            if (type == ExchangeType::FANOUT)
            {
                return true;
            }

            // ---------- TOPIC：news.# 匹配 news.music.pop ----------
            std::vector<std::string> bkeys, rkeys;
            int n_bkey = StrHelper::split(binding_key, ".", bkeys);
            int n_rkey = StrHelper::split(routing_key, ".", rkeys);

            // dp 的行数 = binding 段数 + 1，列数 = routing 段数 + 1
            //  原代码第二维写成了 n_key，routing 段数更多时会越界
            std::vector<std::vector<bool>> dp(n_bkey + 1,
                                              std::vector<bool>(n_rkey + 1, false));
            dp[0][0] = true;

            // 如果 binding_key 以连续的 # 开头，则这些行的第 0 列置 true
            for (int i = 1; i <= n_bkey; i++)
            {
                if (bkeys[i - 1] == "#")
                {
                    dp[i][0] = true;
                    continue;
                }
                break;
            }

            for (int i = 1; i <= n_bkey; i++)
            {
                for (int j = 1; j <= n_rkey; j++)
                {
                    if (bkeys[i - 1] == "*" || bkeys[i - 1] == rkeys[j - 1])
                    {
                        // 单词匹配成功（* 或完全相同）→ 从左上方继承
                        dp[i][j] = dp[i - 1][j - 1];
                    }
                    else if (bkeys[i - 1] == "#")
                    {
                        // # 匹配 0 个或多个单词 --> 从左上、左、上三方继承
                        //  原来写的是 dp[1 - i][j - i] → 负数下标，越界
                        dp[i][j] = dp[i - 1][j - 1] || dp[i][j - 1] || dp[i - 1][j];
                    }
                }
            }
            return dp[n_bkey][n_rkey];
        }
    };
}

#endif