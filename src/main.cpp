#include "db.hpp"

int main(int argc, char* argv[])
{
    Db db("localhost");
    db.main_loop();
}
