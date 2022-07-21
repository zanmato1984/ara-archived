#pragma once

#include "ara/relational/rels.h"

namespace cura::relational {

std::shared_ptr<const Rel> parseJson(const std::string &json);

}
