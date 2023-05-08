

#pragma once

namespace pandawasm {

class Instruction {
public:
    Instruction() = default;
    virtual ~Instruction() = default;

private:
};

namespace instruction {

/// Special instructions

class Noop : public Instruction {};

class Trap : public Instruction {};

class Const : public Instruction {};

/// Variable instructions

class GetLocal : public Instruction {};

class SetLocal : public Instruction {};

class GetGlobal : public Instruction {};

class SetGlobal : public Instruction {};

} // namespace instruction

} // namespace pandawasm
